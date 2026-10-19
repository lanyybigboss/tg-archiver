package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerPool struct {
	cfg    *Config
	store  *Store
	tg     *TGClient
	alert  *Alerter
	active atomic.Int32

	// 作者库：面板改完后热更新，不用重启服务
	authorLib atomic.Pointer[AuthorLib]
	done      atomic.Int64
	failed    atomic.Int64
}

func NewWorkerPool(cfg *Config, st *Store, tg *TGClient, alert *Alerter) *WorkerPool {
	return &WorkerPool{cfg: cfg, store: st, tg: tg, alert: alert}
}

func (p *WorkerPool) Active() int { return int(p.active.Load()) }

// SetAuthorLib 热替换作者库（面板增删作者后调用）。
func (p *WorkerPool) SetAuthorLib(lib *AuthorLib) { p.authorLib.Store(lib) }

// ReloadAuthors 从数据库重新加载作者库。
func (p *WorkerPool) ReloadAuthors() {
	if p.store == nil {
		return
	}
	list, err := p.store.ListAuthors()
	if err != nil {
		slog.Warn("重新加载作者库失败", "err", err)
		return
	}
	p.authorLib.Store(NewAuthorLib(list))
	slog.Info("作者库已更新", "count", len(list))
}
func (p *WorkerPool) DoneN() int64   { return p.done.Load() }
func (p *WorkerPool) FailedN() int64 { return p.failed.Load() }

func (p *WorkerPool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.MaxWorkers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p.loop(ctx, idx)
		}(i)
	}
	wg.Wait()
	slog.Info("worker 池已停止")
}

func (p *WorkerPool) loop(ctx context.Context, idx int) {
	slot := filepath.Join(p.cfg.WorkerDir, fmt.Sprintf("w%d", idx))
	if err := os.MkdirAll(slot, 0o755); err != nil {
		slog.Error("创建 storage 槽失败", "slot", slot, "err", err)
	}
	slog.Info("worker 就绪", "idx", idx, "slot", filepath.Base(slot))

	tk := time.NewTicker(3 * time.Second)
	defer tk.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}

		items, err := p.store.Claim(1)
		if err != nil {
			slog.Error("取任务失败", "err", err)
			continue
		}
		if len(items) == 0 {
			continue
		}
		p.active.Add(1)
		p.process(ctx, items[0], slot)
		p.active.Add(-1)
	}
}

func (p *WorkerPool) process(ctx context.Context, it QueueItem, slot string) {
	log := slog.With("id", it.ID, "tag", it.Tag, "type", it.MediaType, "src", it.Channel)

	dest := ArchiveDir(p.cfg.DownloadDir, it.Tag)
	if err := os.MkdirAll(dest, 0o755); err != nil {
		p.fail(ctx, it, log, "创建归档目录失败: "+err.Error())
		return
	}

	// 每个任务先下到独立暂存目录，完成后再归位：
	// 1) 并发时速度各算各的，不会把隔壁任务的流量算进来
	// 2) 同一归档目录下两个 tdl 不会互相干扰
	stage := filepath.Join(p.cfg.DownloadDir, ".staging", fmt.Sprintf("t%d", it.ID))
	os.RemoveAll(stage)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		p.fail(ctx, it, log, "创建暂存目录失败: "+err.Error())
		return
	}
	defer os.RemoveAll(stage)

	start := time.Now()

	statusID, sendErr := p.tg.Send(ctx, SendOpts{
		ChatID: it.ChatID,
		Text: fmt.Sprintf("开始 #%d  %s / %s\n归档到 %s/",
			it.ID, it.Tag, it.MediaType, filepath.Base(dest)),
	})
	if sendErr != nil {
		log.Warn("发送状态消息失败", "err", sendErr)
	}

	rep := newReporter(p.store, it.ID, p.tg, it.ChatID, statusID, it.Tag, it.MediaType)

	// tdl 的进度输出里没有速度段，只能自己按目录增长实测。
	spdStop := make(chan struct{})
	go p.trackSpeed(it.ID, stage, spdStop)

	dlErr := p.runTDL(ctx, it.URL, stage, slot, rep.onLine)
	close(spdStop)

	if dlErr != nil {
		p.store.SetSpeed(it.ID, "")
		if ctx.Err() != nil {
			// 服务在退出：任务放回队列，不记失败、不减重试额度
			if err := p.store.Requeue(it.ID, "服务重启中断"); err != nil {
				log.Error("放回队列失败", "err", err)
			}
			log.Info("服务退出，任务已放回队列", "id", it.ID)
			rep.finish("已中断", "服务退出，任务将自动续传")
			return
		}
		p.fail(ctx, it, log, dlErr.Error())
		rep.finish("失败", dlErr.Error())
		return
	}

	staged := ListMediaFiles(stage)
	if len(staged) == 0 {
		p.fail(ctx, it, log, "tdl 未产出任何文件")
		rep.finish("未产出文件", "")
		return
	}

	// 归位：先用"理想名"判断是否已存在同文件（去重），再按需避让重名
	var paths []string
	var totalSize int64
	for _, f := range staged {
		stagedInfo, err := os.Stat(f)
		if err != nil {
			continue
		}
		base := filepath.Base(f)

		target := filepath.Join(dest, BuildFileName(base, it.Tag, it.MediaType, p.authorLib.Load(), nil))
		if existInfo, err := os.Stat(target); err == nil {
			if existInfo.Size() == stagedInfo.Size() {
				// 同名同大小 = 同一个文件，丢弃暂存副本
				log.Info("同名同大小已存在，跳过", "file", base)
				os.Remove(f)
				paths = append(paths, target)
				totalSize += existInfo.Size()
				continue
			}
			// 同名但内容不同：加序号避让，绝不覆盖已有文件
			target = filepath.Join(dest, BuildFileName(base, it.Tag, it.MediaType, p.authorLib.Load(), func(n string) bool {
				_, e := os.Stat(filepath.Join(dest, n))
				return e == nil
			}))
		}

		if err := os.Rename(f, target); err != nil {
			log.Error("归位失败", "file", base, "err", err)
			continue
		}
		log.Info("归档", "as", filepath.Base(target))
		paths = append(paths, target)
		totalSize += stagedInfo.Size()
	}

	if len(paths) == 0 {
		p.fail(ctx, it, log, "归档失败：没有文件成功归位")
		rep.finish("归档失败", "")
		return
	}

	if err := p.store.MarkDone(it.ID, strings.Join(paths, "|"), totalSize, it.Tag); err != nil {
		log.Error("写完成状态失败", "err", err)
	}
	p.done.Add(1)

	elapsed := time.Since(start).Round(time.Second)
	log.Info("下载完成", "files", len(paths), "mb", fmt.Sprintf("%.1f", float64(totalSize)/1024/1024), "elapsed", elapsed.String())

	rep.finish("完成", fmt.Sprintf("%d 个文件 · %.1f MB · %s",
		len(paths), float64(totalSize)/1024/1024, elapsed))
}

// fail 统一失败路径：未到重试上限就放回队列并退避，不占着 worker 空转。
// 原版是 sleep(30s) 后再取同一条，把并发位死死占住。
func (p *WorkerPool) fail(ctx context.Context, it QueueItem, log *slog.Logger, reason string) {
	n := it.RetryCount + 1
	if n >= p.cfg.MaxRetry {
		if err := p.store.MarkFailed(it.ID, it.ChatID, it.MsgID, it.Tag, it.URL, reason); err != nil {
			log.Error("写失败状态失败", "err", err)
		}
		p.failed.Add(1)
		log.Error("任务彻底失败", "retry", n, "reason", reason)
		if p.alert != nil {
			p.alert.Emit("error", "task_failed",
				fmt.Sprintf("任务 #%d（%s/%s）重试 %d 次仍失败：%s",
					it.ID, it.Tag, it.MediaType, n, truncateRunes(reason, 300)),
				it.ID, it)
		}

		nctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		p.tg.Send(nctx, SendOpts{
			ChatID: it.ChatID,
			Text: fmt.Sprintf("失败 #%d  %s/%s\n已重试 %d 次，停止。\n%s",
				it.ID, it.Tag, it.MediaType, n, truncateRunes(reason, 200)),
		})
		return
	}

	delay := retryDelay(n)
	if err := p.store.MarkRetry(it.ID, reason, delay); err != nil {
		log.Error("写重试状态失败", "err", err)
	}
	log.Warn("失败，稍后重试", "retry", n, "delay", delay.String(), "reason", truncateRunes(reason, 200))
}

// retryDelay 指数退避：30s、1m、2m、4m、8m，上限 10 分钟。
func retryDelay(n int) time.Duration {
	d := 30 * time.Second
	for i := 1; i < n; i++ {
		d *= 2
		if d >= 10*time.Minute {
			return 10 * time.Minute
		}
	}
	return d
}

// runTDL 调用 tdl 下载。stdout 与 stderr 合并到同一个管道，逐行回调解析进度。
func (p *WorkerPool) runTDL(ctx context.Context, url, dest, slot string, onLine func(string)) error {
	args := []string{
		"dl", "--proxy", p.cfg.TDLProxy, "-u", url, "-d", dest,
		"--template", "{{ .FileName }}", "--skip-same", "--continue",
		"-t", strconv.Itoa(p.cfg.TDLThreads), "-l", "1",
		"--storage", "path=" + slot + ",type=bolt",
	}

	cctx, cancel := context.WithTimeout(ctx, p.cfg.TDLTimeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, p.cfg.TDLBinary, args...)
	setProcGroup(cmd)
	cmd.Cancel = func() error { return killProcessGroup(cmd) }
	cmd.WaitDelay = 5 * time.Second

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		pr.Close()
		return fmt.Errorf("启动 tdl 失败: %w", err)
	}

	waitErr := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		pw.Close()
		waitErr <- err
	}()

	sc := bufio.NewScanner(pr)
	sc.Buffer(make([]byte, 64*1024), 512*1024)
	sc.Split(splitOnCRLF)

	var tail []string
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if onLine != nil {
			onLine(line)
		}
		tail = append(tail, line)
		if len(tail) > 12 {
			tail = tail[1:]
		}
	}
	io.Copy(io.Discard, pr)

	err := <-waitErr
	if err != nil {
		if cctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("下载超时（%s）", p.cfg.TDLTimeout)
		}
		if ctx.Err() != nil {
			return fmt.Errorf("已取消")
		}
		return fmt.Errorf("tdl 退出异常: %v | %s", err, truncateRunes(strings.Join(tail, " / "), 300))
	}
	return nil
}

// splitOnCRLF 进度条用 \r 原地刷新，必须同时按 \r 和 \n 切分才能实时拿到每一帧。
func splitOnCRLF(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

var (
	pctRe   = regexp.MustCompile(`(\d{1,3}(?:\.\d+)?)\s*%`)
	speedRe = regexp.MustCompile(`([\d.]+\s*[KMG]i?B/s)`)
)

func parseProgress(line string) (float64, string, bool) {
	m := pctRe.FindStringSubmatch(line)
	if m == nil {
		return 0, "", false
	}
	pct, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, "", false
	}
	speed := ""
	if s := speedRe.FindStringSubmatch(line); s != nil {
		speed = strings.TrimSpace(s[1])
	}
	return pct, speed, true
}

// ── 进度推送 ─────────────────────────────────────────────

type reporter struct {
	store  *Store
	taskID int64
	tg     *TGClient
	chatID int64
	msgID  int64
	title  string
	start  time.Time

	mu      sync.Mutex
	lastPct float64
	lastAt  time.Time
}

func newReporter(st *Store, taskID int64, tg *TGClient, chatID, msgID int64, tag, mediaType string) *reporter {
	return &reporter{
		store: st, taskID: taskID,
		tg: tg, chatID: chatID, msgID: msgID,
		title: fmt.Sprintf("%s / %s", tag, mediaType),
		start: time.Now(),
	}
}

// onLine 节流后同时更新两处：数据库（面板进度）与 Telegram 消息。
// 只更新 TG 不写库的话，面板里进度会永远停在 0%。
func (r *reporter) onLine(line string) {
	pct, speed, ok := parseProgress(line)
	if !ok {
		return
	}

	r.mu.Lock()
	now := time.Now()
	bigJump := pct-r.lastPct >= 10
	enoughGap := now.Sub(r.lastAt) >= 3*time.Second
	if !bigJump && !enoughGap {
		r.mu.Unlock()
		return
	}
	r.lastPct, r.lastAt = pct, now
	r.mu.Unlock()

	if r.store != nil && r.taskID > 0 {
		r.store.SetProgress(r.taskID, pct, speed)
	}
	if r.msgID == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	// 内容未变时 Telegram 会报错，这里不值得记日志
	_ = r.tg.Edit(ctx, r.chatID, r.msgID, r.render(pct, speed))
}

func (r *reporter) render(pct float64, speed string) string {
	if pct > 100 {
		pct = 100
	}
	filled := int(pct / 10)
	if filled > 10 {
		filled = 10
	}
	bar := strings.Repeat("#", filled) + strings.Repeat(".", 10-filled)

	var b strings.Builder
	fmt.Fprintf(&b, "[%s] %.0f%%\n", bar, pct)
	fmt.Fprintf(&b, "%s\n", r.title)
	if speed != "" {
		fmt.Fprintf(&b, "%s · ", speed)
	}
	fmt.Fprintf(&b, "已用 %s", time.Since(r.start).Round(time.Second))
	return b.String()
}

func (r *reporter) finish(state, detail string) {
	if r.msgID == 0 {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n", state, r.title)
	if detail != "" {
		fmt.Fprintf(&b, "%s\n", detail)
	}
	fmt.Fprintf(&b, "总耗时 %s", time.Since(r.start).Round(time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	_ = r.tg.Edit(ctx, r.chatID, r.msgID, b.String())
}

// trackSpeed 每 3 秒采样归档目录大小，用增量算实际下载速率。
// tdl 的进度行只有百分比、没有速度，面板要显示速度只能靠这个。
func (p *WorkerPool) trackSpeed(id int64, dir string, stop <-chan struct{}) {
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()

	var last int64 = -1
	var lastAt time.Time

	for {
		select {
		case <-stop:
			return
		case <-t.C:
			size := DirSize(dir)
			if last >= 0 {
				if dt := time.Since(lastAt).Seconds(); dt > 0 {
					if delta := size - last; delta > 0 {
						p.store.SetSpeed(id, humanSpeed(float64(delta)/dt))
					} else if delta == 0 {
						p.store.SetSpeed(id, "停滞")
					}
				}
			}
			last, lastAt = size, time.Now()
		}
	}
}

func humanSpeed(bps float64) string {
	switch {
	case bps >= 1024*1024:
		return fmt.Sprintf("%.1f MB/s", bps/1024/1024)
	case bps >= 1024:
		return fmt.Sprintf("%.0f KB/s", bps/1024)
	default:
		return fmt.Sprintf("%.0f B/s", bps)
	}
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// startHeartbeat 保持与旧版一致的心跳文件，方便沿用原有监控习惯。
func startHeartbeat(ctx context.Context, path string, every time.Duration) {
	beat := func() {
		_ = os.WriteFile(path, []byte(strconv.FormatInt(time.Now().Unix(), 10)), 0o644)
	}
	beat()
	tk := time.NewTicker(every)
	go func() {
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				beat()
			}
		}
	}()
}
