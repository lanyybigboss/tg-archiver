package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// lastPollOK 最近一次成功轮询的时间戳（unix）。
// 进程活着、心跳在跳，不代表还在收消息 —— 原版就是栽在"假活"上。
// 健康检查以这个为准，超过阈值就让外部看护重启。
var lastPollOK atomic.Int64

const stalePollSeconds = 300

// rotWriter 按大小轮转日志，避免重演旧版 bot.log 涨到 100MB 且反复打印 token 的问题。
type rotWriter struct {
	mu   sync.Mutex
	path string
	max  int64
	f    *os.File
	size int64
}

func newRotWriter(path string, maxBytes int64) (*rotWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	var size int64
	if info != nil {
		size = info.Size()
	}
	return &rotWriter{path: path, max: maxBytes, f: f, size: size}, nil
}

func (w *rotWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.size+int64(len(p)) > w.max {
		w.f.Close()
		_ = os.Rename(w.path, w.path+".1")
		f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return 0, err
		}
		w.f = f
		w.size = 0
	}
	n, err := w.f.Write(p)
	w.size += int64(n)
	return n, err
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func userTagKey(uid int64) string { return fmt.Sprintf("tag:%d", uid) }

func getTag(st *Store, uid int64) string {
	if v := st.GetState(userTagKey(uid), ""); v != "" {
		return v
	}
	return "未分类"
}

func setTag(st *Store, uid int64, tag string) {
	_ = st.SetState(userTagKey(uid), tag)
}

// handleUpdate 处理单条 update。轮询线程串行调用，无需额外加锁。
func handleUpdate(ctx context.Context, cfg *Config, st *Store, tg *TGClient, u Update) {
	m := u.Message
	if m == nil {
		return
	}
	if cfg.OwnerID > 0 {
		if m.From == nil || m.From.ID != cfg.OwnerID {
			return
		}
	}
	uid := m.From.ID

	reply := func(text string) {
		rctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := tg.Send(rctx, SendOpts{ChatID: m.Chat.ID, Text: text}); err != nil {
			slog.Warn("回复失败", "err", err)
		}
	}

	text := strings.TrimSpace(m.Text)
	cmd := ""
	if strings.HasPrefix(text, "/") {
		cmd = strings.ToLower(strings.Fields(text)[0])
		if i := strings.Index(cmd, "@"); i > 0 {
			cmd = cmd[:i]
		}
	}

	switch cmd {
	case "/start", "/help":
		reply("TG 下载 Bot\n\n" +
			"#标签名   设置当前分类\n" +
			"/tag      查看当前标签\n" +
			"/reset    重置为未分类\n" +
			"/status   队列统计\n" +
			"/queue    最近任务\n" +
			"/retry ID 重试失败任务\n" +
			"/panel    管理面板地址\n\n" +
			"直接转发频道/群组的媒体消息给我即可入队。")
		return

	case "/tag":
		reply("当前标签: " + getTag(st, uid))
		return

	case "/reset":
		setTag(st, uid, "未分类")
		reply("已重置为「未分类」")
		return

	case "/status":
		s, err := st.Stats()
		if err != nil {
			reply("读取统计失败: " + err.Error())
			return
		}
		writeJSONStats := func() string {
			return fmt.Sprintf("标签: %s\n待处理: %d\n进行中: %d\n完成: %d\n失败: %d\n占用: %.1f GB",
				getTag(st, uid), s.Pending, s.Processing, s.Done, s.Failed,
				float64(DirSize(cfg.DownloadDir))/1024/1024/1024)
		}
		reply(writeJSONStats())
		return

	case "/queue":
		items, err := st.ListQueue("all", 10)
		if err != nil {
			reply("读取队列失败: " + err.Error())
			return
		}
		if len(items) == 0 {
			reply("队列为空")
			return
		}
		var b strings.Builder
		b.WriteString("最近 10 条任务\n")
		for _, it := range items {
			fmt.Fprintf(&b, "#%d %s %s/%s %d%%\n",
				it.ID, it.Status, it.Tag, it.MediaType, int(it.Progress))
		}
		reply(b.String())
		return

	case "/retry":
		parts := strings.Fields(text)
		if len(parts) < 2 {
			reply("用法: /retry 任务ID")
			return
		}
		id, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			reply("ID 不合法")
			return
		}
		if err := st.SetStatus(id, "pending"); err != nil {
			reply("重试失败: " + err.Error())
			return
		}
		reply(fmt.Sprintf("已重新入队 #%d", id))
		return

	case "/panel":
		reply("管理面板: http://" + cfg.PanelAddr + "/")
		return
	}

	// #标签
	if strings.HasPrefix(text, "#") {
		tag := strings.TrimSpace(strings.TrimPrefix(text, "#"))
		if tag == "" {
			reply("用法: #标签名")
			return
		}
		setTag(st, uid, tag)

		// 标签就是作者名，顺手入库，省得再手工维护一遍
		added := false
		if len([]rune(tag)) >= 2 && tag != "未分类" {
			if _, err := st.AddAuthor(tag, "", "", "来自标签命令"); err == nil {
				added = true
			}
		}
		if added {
			reply("标签已设为: " + tag + "（已加入作者库）")
		} else {
			reply("标签已设为: " + tag)
		}
		return
	}

	// 转发媒体
	kind := m.MediaKind()
	if kind == "" {
		return
	}
	channel, link := m.Source()
	if link == "" {
		reply("无法构造链接：这条消息不是从频道/群组转发的（私聊转发没有可定位的消息 ID）")
		return
	}
	if st.IsDuplicate(m.Chat.ID, m.MessageID) {
		// 静默忽略。offset 丢失重启时 Telegram 会重投积压消息，逐条回复"已处理过"会刷屏。
		slog.Debug("重复消息，跳过", "msg_id", m.MessageID)
		return
	}

	tag := getTag(st, uid)
	id, err := st.Enqueue(m.Chat.ID, m.MessageID, tag, channel, kind, link)
	if err != nil {
		reply("入队失败: " + err.Error())
		return
	}
	_ = st.AddHistory(m.Chat.ID, m.MessageID, tag, channel)

	label := tag + " / " + kind
	if channel != "" {
		label += " (" + channel + ")"
	}
	reply(fmt.Sprintf("已入队 #%d  %s", id, label))
	slog.Info("入队", "id", id, "tag", tag, "kind", kind, "channel", channel, "url", link)
}

// runPoller 长轮询。offset 落库，重启后不会重复拉取已处理的消息。
func runPoller(ctx context.Context, cfg *Config, st *Store, tg *TGClient, alert *Alerter) {
	var offset int64
	if v := st.GetState("offset", "0"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// 静默归零会导致重放全部积压消息，必须留下痕迹
			slog.Warn("offset 解析失败，将从 0 开始（可能重放积压消息）", "value", v, "err", err)
		} else {
			offset = n
		}
	}
	slog.Info("轮询启动", "offset", offset, "poll_timeout", cfg.PollTimeout)

	consecutiveFails := 0
	for ctx.Err() == nil {
		ups, err := tg.GetUpdates(ctx, offset, cfg.PollTimeout, 100)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if wait, ok := AsRateLimited(err); ok {
				slog.Warn("触发限流，按服务端要求等待", "wait", wait.String())
				sleepCtx(ctx, wait)
				continue
			}
			wait := tg.Backoff()
			if wait <= 0 {
				wait = time.Second
			}
			fails, rebuilds, _ := tg.Health()
			consecutiveFails++
			slog.Error("轮询失败", "err", err, "retry_in", wait.String(),
				"consecutive", fails, "pool_rebuilds", rebuilds)

			// 连不上 Telegram 是用户最该知道的故障，达阈值报一次，之后每 20 次再提醒
			if alert != nil && (consecutiveFails == 5 || consecutiveFails%20 == 0) {
				alert.Emit("warn", "poll_error",
					fmt.Sprintf("轮询连续失败 %d 次，连接池已重建 %d 次：%s",
						consecutiveFails, rebuilds, truncateRunes(err.Error(), 300)), 0, nil)
			}
			sleepCtx(ctx, wait)
			continue
		}

		for _, u := range ups {
			offset = u.UpdateID + 1
			handleUpdate(ctx, cfg, st, tg, u)
		}
		lastPollOK.Store(time.Now().Unix())
		if consecutiveFails >= 5 && alert != nil {
			alert.Emit("info", "poll_recovered",
				fmt.Sprintf("轮询已恢复（此前连续失败 %d 次）", consecutiveFails), 0, nil)
		}
		consecutiveFails = 0
		if len(ups) > 0 {
			if err := st.SetState("offset", strconv.FormatInt(offset, 10)); err != nil {
				slog.Warn("保存 offset 失败", "err", err)
			}
		}
	}
}

func main() {
	logPath := "/opt/tg-archiver/data/bot.log"
	if v := os.Getenv("BASE_DIR"); v != "" {
		logPath = v + "/bot.log"
	}

	var out io.Writer = os.Stdout
	if rw, err := newRotWriter(logPath, 50*1024*1024); err == nil {
		out = io.MultiWriter(os.Stdout, rw)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := loadConfig()
	if err != nil {
		slog.Error("配置加载失败", "err", err)
		os.Exit(1)
	}

	for _, d := range []string{cfg.BaseDir, cfg.DownloadDir, cfg.WorkerDir, cfg.LogDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			slog.Error("创建目录失败", "dir", d, "err", err)
			os.Exit(1)
		}
	}

	st, err := OpenStore(cfg.DBPath)
	if err != nil {
		slog.Error("打开数据库失败", "err", err, "path", cfg.DBPath)
		os.Exit(1)
	}
	defer st.Close()

	if n, err := st.ResetStale(); err == nil && n > 0 {
		slog.Info("已把上次残留的进行中任务放回队列", "count", n)
	}

	// 首次运行用历史标签初始化作者库（标签本来就是手动分类的博主名）
	if n, err := st.SeedAuthorsFromTags(); err == nil && n > 0 {
		slog.Info("作者库已用历史标签初始化", "count", n)
	}

	tg := NewTGClient(cfg.BotToken, cfg.HTTPProxy)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bootCtx, cancelBoot := context.WithTimeout(ctx, 30*time.Second)
	me, err := tg.GetMe(bootCtx)
	cancelBoot()
	if err != nil {
		// 不要 exit：节点漂移是常态，退出只会让 systemd 无限重启，
		// 面板也起不来，反而看不到到底出了什么问题。让 poller 自己退避重试。
		slog.Error("Telegram 暂时连不上，面板照常启动，后台持续重试", "err", err)
	} else {
		slog.Info("Bot 已连接", "username", me.Username, "id", me.ID)
		lastPollOK.Store(time.Now().Unix())
	}

	alert := NewAlerter(st, cfg.AlertWebhook, cfg.AlertWebhookFormat, cfg.PanelURL)
	pool := NewWorkerPool(cfg, st, tg, alert)
	pool.ReloadAuthors()
	panel := NewPanel(cfg, st, tg, pool, alert)

	srv := newHTTPServer(cfg.PanelAddr, panel.Handler())
	go func() {
		slog.Info("面板已启动", "addr", cfg.PanelAddr)
		if err := srv.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			slog.Error("面板异常退出", "err", err)
		}
	}()

	startHeartbeat(ctx, cfg.BaseDir+"/.heartbeat", 30*time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		pool.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		NewNodeTester(cfg, pool, alert, cfg.MihomoAPI, cfg.NodeGroup).Run(ctx)
	}()

	slog.Info("服务就绪",
		"workers", cfg.MaxWorkers,
		"tdl_threads", cfg.TDLThreads,
		"cache_gb", cfg.CacheLimitGB,
		"tdl_timeout", cfg.TDLTimeout.String(),
		"base", cfg.BaseDir)

	runPoller(ctx, cfg, st, tg, alert)

	slog.Info("收到停止信号，正在退出…")
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	wg.Wait()
	slog.Info("已退出")
}
