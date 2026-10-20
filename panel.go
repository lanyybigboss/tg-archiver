package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

type Panel struct {
	cfg   *Config
	store *Store
	tg    *TGClient
	pool  *WorkerPool
	alert *Alerter
}

func NewPanel(cfg *Config, st *Store, tg *TGClient, pool *WorkerPool, alert *Alerter) *Panel {
	return &Panel{cfg: cfg, store: st, tg: tg, pool: pool, alert: alert}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (p *Panel) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if p.cfg.PanelToken != "" {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if strings.TrimSpace(tok) != p.cfg.PanelToken {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
				return
			}
		}
		next(w, r)
	}
}

func (p *Panel) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		last := lastPollOK.Load()
		// last==0 表示从启动至今一次都没成功轮询过。
		// 此时 age = now - 0 会是个几十亿的巨大值，必须单独处理，
		// 否则 poll_age 显示成 1792462292 这种看不懂的数字。
		if last == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "stale never_polled_ok\n")
			return
		}
		age := time.Now().Unix() - last
		if age > stalePollSeconds {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "stale last_poll_ok%ds_ago\n", age)
			return
		}
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("GET /api/stats", p.auth(func(w http.ResponseWriter, r *http.Request) {
		st, err := p.store.Stats()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		st.DiskBytes = DirSize(p.cfg.DownloadDir)
		fails, rebuilds, lastErr := p.tg.Health()
		pollAge := int64(-1)
		if last := lastPollOK.Load(); last > 0 {
			pollAge = time.Now().Unix() - last
		}
		writeJSON(w, 200, map[string]any{
			"stats":          st,
			"active":         p.pool.Active(),
			"workers":        p.cfg.MaxWorkers,
			"done_total":     p.pool.DoneN(),
			"fail_total":     p.pool.FailedN(),
			"cache_limit":    p.cfg.CacheLimitGB,
			"tg_fails":       fails,
			"tg_rebuilds":    rebuilds,
			"tg_last_err":    lastErr,
			"poll_age":       pollAge,
			"poll_stale":     pollAge < 0 || pollAge > stalePollSeconds,
			"server_time":    time.Now().Format("2006-01-02 15:04:05"),
			"auth_on":        p.cfg.PanelToken != "",
			"unacked_events": p.store.UnackedEventCount(),
		})
	}))

	mux.HandleFunc("GET /api/queue", p.auth(func(w http.ResponseWriter, r *http.Request) {
		status := r.URL.Query().Get("status")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		items, err := p.store.ListQueue(status, limit)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"items": items, "count": len(items)})
	}))

	mux.HandleFunc("POST /api/queue/{id}/retry", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		if err := p.store.SetStatus(id, "pending"); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		slog.Info("面板：任务重新入队", "id", id)
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	mux.HandleFunc("POST /api/queue/{id}/cancel", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		it, err := p.store.Get(id)
		if err != nil {
			writeJSON(w, 404, map[string]string{"error": "任务不存在"})
			return
		}
		if err := p.store.MarkFailed(id, it.ChatID, it.MsgID, it.Tag, it.URL, "面板手动取消"); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		slog.Info("面板：任务已取消", "id", id)
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	mux.HandleFunc("POST /api/queue/{id}/tag", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		var body struct {
			Tag string `json:"tag"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Tag) == "" {
			writeJSON(w, 400, map[string]string{"error": "标签不能为空"})
			return
		}
		if _, err := p.store.db.Exec(`UPDATE queue SET tag=?, updated_at=? WHERE id=?`,
			strings.TrimSpace(body.Tag), dbNow(), id); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		slog.Info("面板：修改标签", "id", id, "tag", body.Tag)
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	// ── 作者库 ──────────────────────────────────────────
	mux.HandleFunc("GET /api/authors", p.auth(func(w http.ResponseWriter, r *http.Request) {
		list, err := p.store.ListAuthors()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"items": list, "count": len(list)})
	}))

	mux.HandleFunc("POST /api/authors", p.auth(func(w http.ResponseWriter, r *http.Request) {
		var b Author
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			writeJSON(w, 400, map[string]string{"error": "请求体解析失败"})
			return
		}
		id, err := p.store.AddAuthor(b.Name, b.Aliases, b.Channel, b.Note)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		p.pool.ReloadAuthors()
		writeJSON(w, 200, map[string]any{"ok": true, "id": id})
	}))

	mux.HandleFunc("PUT /api/authors/{id}", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		var b Author
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			writeJSON(w, 400, map[string]string{"error": "请求体解析失败"})
			return
		}
		if err := p.store.UpdateAuthor(id, b.Name, b.Aliases, b.Channel, b.Note); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		p.pool.ReloadAuthors()
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	mux.HandleFunc("DELETE /api/authors/{id}", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		if err := p.store.DeleteAuthor(id); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		p.pool.ReloadAuthors()
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	// 批量导入：每行一个，支持 "作者名" 或 "作者名,别名1,别名2"
	// 空行、# 开头的注释行忽略；已存在的跳过
	mux.HandleFunc("POST /api/authors/bulk", p.auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, 400, map[string]string{"error": "请求体解析失败"})
			return
		}
		var added, skipped int
		var bad []string
		for _, line := range strings.Split(body.Text, "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "\ufeff"))
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			fields := strings.FieldsFunc(line, func(c rune) bool {
				return c == ',' || c == '，' || c == '\t' || c == '|'
			})
			if len(fields) == 0 {
				continue
			}
			name := strings.TrimSpace(fields[0])
			if name == "" {
				continue
			}
			var aliases []string
			for _, f := range fields[1:] {
				if a := strings.TrimSpace(f); a != "" {
					aliases = append(aliases, a)
				}
			}
			id, err := p.store.AddAuthor(name, strings.Join(aliases, ","), "", "批量导入")
			if err != nil || id == 0 {
				skipped++
				if err != nil {
					bad = append(bad, name+": "+err.Error())
				}
				continue
			}
			added++
		}
		p.pool.ReloadAuthors()
		writeJSON(w, 200, map[string]any{
			"ok": true, "added": added, "skipped": skipped, "errors": bad,
			"total": p.pool.authorLib.Load().Len(),
		})
	}))

	// 把当前所有标签中尚未入库的补进作者库
	mux.HandleFunc("POST /api/authors/sync_tags", p.auth(func(w http.ResponseWriter, r *http.Request) {
		n, err := p.store.SeedAuthorsFromTagsForce()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		p.pool.ReloadAuthors()
		writeJSON(w, 200, map[string]any{"ok": true, "added": n, "total": p.pool.authorLib.Load().Len()})
	}))

	// 预览：给一个原始文件名 + 标签，看会解析成什么名字（不落盘）
	mux.HandleFunc("GET /api/authors/preview", p.auth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		name := q.Get("name")
		tag := q.Get("tag")
		media := q.Get("media")
		if media == "" {
			media = "视频"
		}
		lib := p.pool.authorLib.Load()
		resolved := ""
		if lib != nil {
			resolved = lib.Resolve(name, tag)
		}
		writeJSON(w, 200, map[string]any{
			"resolved_author": resolved,
			"final_name":      BuildFileName(name, tag, media, lib, nil),
			"author_count":    lib.Len(),
		})
	}))

	// ── 事件接口：给 NAS 上的 agent 用 ──────────────────
	// GET /api/events?since=<last_id>&limit=100&unacked=1
	mux.HandleFunc("GET /api/events", p.auth(func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		unacked := r.URL.Query().Get("unacked") == "1"
		items, err := p.store.ListEvents(since, limit, unacked)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		var maxID int64 = since
		for _, e := range items {
			if e.ID > maxID {
				maxID = e.ID
			}
		}
		writeJSON(w, 200, map[string]any{
			"items": items, "count": len(items),
			"next_since": maxID, "unacked": p.store.UnackedEventCount(),
		})
	}))

	// POST /api/events/{id}/ack
	mux.HandleFunc("POST /api/events/{id}/ack", p.auth(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "id 非法"})
			return
		}
		if err := p.store.AckEvent(id); err != nil {
			writeJSON(w, 404, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	// POST /api/events/ack_all
	mux.HandleFunc("POST /api/events/ack_all", p.auth(func(w http.ResponseWriter, r *http.Request) {
		n, err := p.store.AckAllEvents()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true, "acked": n})
	}))

	// POST /api/alert  让 agent（或人）也能主动写入一条事件，便于统一告警出口
	mux.HandleFunc("POST /api/alert", p.auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Level   string `json:"level"`
			Kind    string `json:"kind"`
			Message string `json:"message"`
			TaskID  int64  `json:"task_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Message) == "" {
			writeJSON(w, 400, map[string]string{"error": "message 不能为空"})
			return
		}
		if body.Level == "" {
			body.Level = "info"
		}
		if body.Kind == "" {
			body.Kind = "external"
		}
		if p.alert != nil {
			p.alert.Emit(body.Level, body.Kind, body.Message, body.TaskID, nil)
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	mux.HandleFunc("POST /api/cleanup", p.auth(func(w http.ResponseWriter, r *http.Request) {
		before := DirSize(p.cfg.DownloadDir)
		go CleanupCache(p.cfg, p.store)
		writeJSON(w, 200, map[string]any{"ok": true, "before_bytes": before})
	}))

	mux.HandleFunc("POST /api/notify", p.auth(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text string `json:"text"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if strings.TrimSpace(body.Text) == "" || p.cfg.OwnerID == 0 {
			writeJSON(w, 400, map[string]string{"error": "内容为空或未配置 OWNER_ID"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		if _, err := p.tg.Send(ctx, SendOpts{ChatID: p.cfg.OwnerID, Text: body.Text}); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
	}))

	return mux
}

func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}
