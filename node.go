package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// NodeTester 定期实测各节点带宽，把 mihomo 的策略组切到最快的一个。
//
// 为什么需要它：mihomo 的「自动选择」是 URLTest，只按延迟挑节点，完全不看带宽，
// 于是长期停在延迟低但很窄的节点上。实测 17 个节点里最快与最慢差 4 倍以上。
//
// 测速必须在没有下载任务时进行，否则带宽被抢占、数据失真，所以先等空闲。
type NodeTester struct {
	cfg    *Config
	pool   *WorkerPool
	alert  *Alerter
	apiURL string
	group  string
	client *http.Client

	lastBest  string
	lastSpeed float64
	lastAt    time.Time
}

const speedTestURL = "https://speed.cloudflare.com/__down?bytes=30000000"

func NewNodeTester(cfg *Config, pool *WorkerPool, alert *Alerter, apiURL, group string) *NodeTester {
	if apiURL == "" {
		apiURL = "http://127.0.0.1:9090"
	}
	if group == "" {
		group = "节点选择"
	}
	tr := &http.Transport{
		Proxy:               func(*http.Request) (*url.URL, error) { return url.Parse(cfg.HTTPProxy) },
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
	}
	// API 请求不走代理（见 apiClient），测速请求走代理
	return &NodeTester{
		cfg: cfg, pool: pool, alert: alert,
		apiURL: strings.TrimRight(apiURL, "/"),
		group:  group,
		client: &http.Client{Transport: tr},
	}
}

func (t *NodeTester) apiClient() *http.Client {
	return &http.Client{
		Timeout:   8 * time.Second,
		Transport: &http.Transport{Proxy: nil},
	}
}

func (t *NodeTester) Run(ctx context.Context) {
	if !t.cfg.NodeTestEnabled {
		slog.Info("节点测速已关闭")
		return
	}
	slog.Info("节点测速已启用", "interval", t.cfg.NodeTestInterval.String(), "group", t.group)

	// 启动后先等一会儿，让轮询和首个任务稳定下来
	select {
	case <-ctx.Done():
		return
	case <-time.After(90 * time.Second):
	}

	t.Once(ctx)

	tk := time.NewTicker(t.cfg.NodeTestInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.Once(ctx)
		}
	}
}

// EnsureReachable 应急救援：确认当前节点还能连 Telegram，不能就换一个。
//
// 由轮询失败路径调用（Telegram 连不上时最该做的事就是换出口），
// 与周期测速无关，不等固定间隔、不受下载任务占用带宽的限制。
// 返回 true 表示当前（或新切换的）节点确认可连 Telegram。
func (t *NodeTester) EnsureReachable(ctx context.Context) bool {
	// 当前节点先试一次，避免无谓的切换
	if t.telegramReachable() {
		return true
	}

	slog.Warn("当前节点连不上 Telegram，启动应急换节点", "group", t.group)

	name, ok := t.pickReachable()
	if !ok {
		slog.Error("应急救援失败：所有节点都连不上 Telegram")
		return false
	}

	if err := t.switchTo(name); err != nil {
		slog.Error("应急救援切换节点失败", "node", name, "err", err)
		return false
	}
	time.Sleep(600 * time.Millisecond)

	if !t.telegramReachable() {
		slog.Error("应急救援后仍连不上 Telegram", "node", name)
		return false
	}

	slog.Info("应急救援成功，已切到可连 Telegram 的节点", "node", name)
	t.lastBest, t.lastAt = name, time.Now()
	if t.alert != nil {
		t.alert.Emit("warn", "node_emergency",
			fmt.Sprintf("Telegram 连不上，已自动切换代理节点到 %s。", name), 0,
			map[string]any{"node": name})
	}
	return true
}

// pickReachable 用 mihomo 的 per-node delay 接口逐个测「该节点 → api.telegram.org」
// 的连通性，返回一个能通 Telegram 的节点名。
//
// 与 measure() 的区别：
//   - measure() 走 Cloudflare，测的是带宽，且需要切换策略组（会短暂影响现网流量）
//   - 这里走 mihomo 的 delay 接口，直接指定单个节点测，且目标就是 Telegram 本身
//
// 因此这是判断「这个节点能不能用」的权威依据。
func (t *NodeTester) pickReachable() (string, bool) {
	cands, err := t.candidates()
	if err != nil {
		slog.Warn("读取节点列表失败", "err", err)
		return "", false
	}
	if len(cands) == 0 {
		return "", false
	}

	type delayResult struct {
		name  string
		delay int
	}
	var ok []delayResult

	for _, name := range cands {
		u := fmt.Sprintf("%s/proxies/%s/delay?timeout=5000&url=%s",
			t.apiURL, url.PathEscape(name), url.QueryEscape("https://api.telegram.org/"))
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := t.apiClient().Do(req)
		if err != nil {
			continue
		}
		var payload struct {
			Delay int `json:"delay"`
		}
		decErr := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&payload)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if decErr != nil {
			continue
		}
		if payload.Delay > 0 {
			ok = append(ok, delayResult{name, payload.Delay})
		}
	}

	if len(ok) == 0 {
		return "", false
	}
	// 延迟低的优先
	sort.Slice(ok, func(i, j int) bool { return ok[i].delay < ok[j].delay })
	slog.Info("可达性扫描完成", "reachable", len(ok), "of", len(cands),
		"best", ok[0].name, "delay_ms", ok[0].delay)
	return ok[0].name, true
}

// Once 执行一轮：等空闲 → 逐节点测速 → 切到最快。
func (t *NodeTester) Once(ctx context.Context) {
	if !t.waitIdle(ctx) {
		slog.Info("节点测速跳过：下载任务一直占用带宽")
		return
	}

	cands, err := t.candidatesWith(t.bandwidthRegions())
	if err != nil {
		slog.Warn("读取节点列表失败", "err", err)
		return
	}
	if len(cands) == 0 {
		slog.Warn("没有可测节点（检查 MIHOMO_API 与策略组名）")
		return
	}

	slog.Info("开始节点测速", "count", len(cands))
	type result struct {
		name  string
		speed float64
	}
	var results []result

	for _, name := range cands {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := t.switchTo(name); err != nil {
			continue
		}
		time.Sleep(600 * time.Millisecond) // 等新连接生效
		// 先预热 1 秒再计时 4 秒：3 秒窗口里 TCP 慢启动占比太大，
		// 曾把能跑 10 MB/s 的节点测成 0.16（相差 60 倍）
		spd := t.measure(1*time.Second, 4*time.Second)
		results = append(results, result{name, spd})
		slog.Info("节点测速", "node", name, "mb_s", fmt.Sprintf("%.2f", spd))
	}

	sort.Slice(results, func(i, j int) bool { return results[i].speed > results[j].speed })
	if len(results) == 0 {
		return
	}

	// 带宽高 ≠ 能连 Telegram。测速走的是 Cloudflare，某些节点对 TG 是死的。
	//
	// 关键：可达性优先于带宽。
	// 早先的实现在「最快的 3 个都连不上 TG」时选择「保持当前节点不动」，
	// 可当前节点恰恰就是那个死节点 —— 于是永远锁死在一个不通的出口上，
	// 表现为进程活着但连续几小时下载 0（2026-10-20 06:00 起即为此症状）。
	// 现在改成：先按可达性过滤，剩下候选里再按带宽排序。
	var best result
	found := false
	for _, r := range results {
		if r.speed <= 0 {
			continue
		}
		if err := t.switchTo(r.name); err != nil {
			continue
		}
		time.Sleep(500 * time.Millisecond)
		if t.telegramReachable() {
			best, found = r, true
			break
		}
		slog.Warn("节点带宽够但连不上 Telegram，跳过", "node", r.name, "mb_s", fmt.Sprintf("%.2f", r.speed))
	}
	if !found {
		// 到这一步说明所有测过速的节点都连不上 TG。此时不能原地不动，
		// 换用 mihomo 的 per-node delay 接口（不需要切换策略组）再扫一遍，
		// 找出真正能通 TG 的节点；只要有一个能通就切过去。
		slog.Warn("所有测速节点都连不上 Telegram，改用可达性接口重扫")
		if name, ok := t.pickReachable(); ok {
			if err := t.switchTo(name); err == nil {
				time.Sleep(500 * time.Millisecond)
				if t.telegramReachable() {
					best, found = result{name: name, speed: 0}, true
					slog.Info("已切到可连 Telegram 的节点（带宽未知）", "node", name)
				}
			}
		}
	}
	if !found {
		slog.Error("没有任何节点能连上 Telegram，保持当前节点不动", "node", t.group)
		return
	}

	if err := t.switchTo(best.name); err != nil {
		slog.Error("切换节点失败", "node", best.name, "err", err)
		return
	}
	time.Sleep(400 * time.Millisecond)

	prev := t.lastBest
	t.lastBest, t.lastSpeed, t.lastAt = best.name, best.speed, time.Now()

	slog.Info("节点已切到最快", "node", best.name, "mb_s", fmt.Sprintf("%.2f", best.speed))

	// 只在节点发生变化时上报，避免每 3 小时刷一条
	if t.alert != nil && best.name != prev {
		top := results[:3]
		var sb strings.Builder
		for _, r := range top {
			fmt.Fprintf(&sb, "%s %.2f MB/s；", r.name, r.speed)
		}
		t.alert.Emit("info", "node_switch",
			fmt.Sprintf("代理节点切换到 %s（%.2f MB/s）。前三名：%s",
				best.name, best.speed, strings.TrimSuffix(sb.String(), "；")),
			0, map[string]any{"best": best.name, "speed": best.speed})
	}
}

// waitIdle 等到没有下载任务；最多等 20 分钟，仍忙则放弃本轮。
func (t *NodeTester) waitIdle(ctx context.Context) bool {
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		if t.pool == nil || t.pool.Active() == 0 {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(30 * time.Second):
		}
	}
	return t.pool == nil || t.pool.Active() == 0
}

var nonNodeTypes = map[string]bool{
	"Selector": true, "URLTest": true, "Fallback": true, "LoadBalance": true,
	"Direct": true, "Reject": true, "RejectDrop": true, "Compatible": true,
	"Pass": true, "Relay": true, "Dns": true,
}

// bandwidthRegions 带宽测速时只覆盖这些地区。
// 逐个切换策略组测速会短暂影响共用该策略组的其他服务，
// 所以测速范围收窄；应急救援（EnsureReachable）则扫全部节点。
func (t *NodeTester) bandwidthRegions() []string {
	return []string{"Hong Kong", "Japan", "Singapore", "Taiwan"}
}

func (t *NodeTester) candidates() ([]string, error) {
	return t.candidatesWith(nil)
}

// candidatesWith 返回订阅里所有真实节点名。
// regionOnly 非 nil 时，只保留名称命中这些地区关键词的节点（带宽测速用）。
func (t *NodeTester) candidatesWith(regionOnly []string) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, t.apiURL+"/proxies", nil)
	if err != nil {
		return nil, err
	}
	resp, err := t.apiClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var payload struct {
		Proxies map[string]struct {
			Type string `json:"type"`
		} `json:"proxies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	var out []string
	for name, p := range payload.Proxies {
		if nonNodeTypes[p.Type] {
			continue
		}
		// 订阅里混着「剩余流量：xxx」这类信息节点，跳过
		if strings.Contains(name, "剩余流量") || strings.Contains(name, "套餐") ||
			strings.Contains(name, "距离下次") {
			continue
		}
		if regionOnly != nil {
			matched := false
			for _, k := range regionOnly {
				if strings.Contains(name, k) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func (t *NodeTester) switchTo(name string) error {
	body := strings.NewReader(fmt.Sprintf(`{"name":%q}`, name))
	req, err := http.NewRequest(http.MethodPut,
		t.apiURL+"/proxies/"+url.PathEscape(t.group), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.apiClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 512))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// telegramReachable 用当前节点请求一次 Telegram，判断真的能用。
func (t *NodeTester) telegramReachable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.telegram.org/", nil)
	if err != nil {
		return false
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
	return resp.StatusCode > 0
}

// measure 先预热一段时间丢弃，再正式计时算 MB/s。
// 不预热的话 TCP 慢启动会把宽带节点测成窄带，选出来的节点是错的。
func (t *NodeTester) measure(warmup, d time.Duration) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), warmup+d+6*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, speedTestURL, nil)
	if err != nil {
		return 0
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	buf := make([]byte, 64*1024)

	warmUntil := time.Now().Add(warmup)
	for time.Now().Before(warmUntil) {
		if _, err := resp.Body.Read(buf); err != nil {
			return 0
		}
	}

	start := time.Now()
	var total int64
	for time.Since(start) < d {
		n, err := resp.Body.Read(buf)
		total += int64(n)
		if err != nil {
			break
		}
	}
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(total) / elapsed / 1024 / 1024
}
