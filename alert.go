package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Alerter 把异常同时写到两个地方：
//  1. events 表 —— agent 通过 GET /api/events 拉取，可查历史、可 ack
//  2. webhook   —— 配置了就主动推送，走内网、不经过代理
type Alerter struct {
	store   *Store
	webhook string
	format  string
	panel   string
	client  *http.Client
}

func NewAlerter(st *Store, webhook, format, panelURL string) *Alerter {
	if format == "" {
		format = "generic"
	}
	return &Alerter{
		store:   st,
		webhook: webhook,
		format:  format,
		panel:   panelURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
			// webhook 一般指向内网 agent，必须绕开 NAS 上的 http_proxy
			Transport: &http.Transport{Proxy: nil},
		},
	}
}

// Emit 记录事件；配置了 webhook 时异步推送，不阻塞调用方（通常是下载线程）。
func (a *Alerter) Emit(level, kind, message string, taskID int64, payload any) {
	pl := ""
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			pl = string(b)
		}
	}

	id, err := a.store.AddEvent(level, kind, message, taskID, pl)
	if err != nil {
		slog.Warn("写事件失败", "err", err)
		return
	}
	slog.Warn("事件已记录", "id", id, "level", level, "kind", kind, "detail", truncateRunes(message, 160))

	if a.webhook == "" {
		return
	}
	go a.push(id, level, kind, message, taskID, pl)
}

func (a *Alerter) push(id int64, level, kind, message string, taskID int64, payload string) {
	text := fmt.Sprintf("[%s] %s\n%s", level, kind, message)
	if taskID > 0 {
		text += fmt.Sprintf("\n任务 #%d", taskID)
	}
	if a.panel != "" {
		text += "\n" + a.panel
	}

	var body []byte
	switch a.format {
	case "feishu":
		// 飞书自定义机器人：{"msg_type":"text","content":{"text":"..."}}
		body, _ = json.Marshal(map[string]any{
			"msg_type": "text",
			"content":  map[string]string{"text": text},
		})
	default:
		body, _ = json.Marshal(map[string]any{
			"id":      id,
			"level":   level,
			"kind":    kind,
			"message": message,
			"task_id": taskID,
			"payload": payload,
			"panel":   a.panel,
			"time":    time.Now().Format(time.RFC3339),
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.webhook, bytes.NewReader(body))
	if err != nil {
		slog.Warn("webhook 请求构造失败", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		slog.Warn("webhook 推送失败", "err", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		a.store.MarkEventPushed(id)
		return
	}
	slog.Warn("webhook 返回异常", "code", resp.StatusCode)
}
