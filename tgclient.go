package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── Bot API 数据结构（只声明用得到的字段）─────────────────

type User struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	FirstName string `json:"first_name"`
	Username  string `json:"username"`
}

type Chat struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Title     string `json:"title"`
	Username  string `json:"username"`
	FirstName string `json:"first_name"`
}

type ForwardOrigin struct {
	Type            string `json:"type"`
	Date            int64  `json:"date"`
	Chat            *Chat  `json:"chat"`
	SenderChat      *Chat  `json:"sender_chat"`
	MessageID       int64  `json:"message_id"`
	SenderUser      *User  `json:"sender_user"`
	SenderUserName  string `json:"sender_user_name"`
	AuthorSignature string `json:"author_signature"`
}

type Message struct {
	MessageID     int64           `json:"message_id"`
	Date          int64           `json:"date"`
	Chat          Chat            `json:"chat"`
	From          *User           `json:"from"`
	Text          string          `json:"text"`
	Caption       string          `json:"caption"`
	Video         json.RawMessage `json:"video"`
	VideoNote     json.RawMessage `json:"video_note"`
	Animation     json.RawMessage `json:"animation"`
	Photo         json.RawMessage `json:"photo"`
	Sticker       json.RawMessage `json:"sticker"`
	Document      json.RawMessage `json:"document"`
	Audio         json.RawMessage `json:"audio"`
	Voice         json.RawMessage `json:"voice"`
	ForwardOrigin *ForwardOrigin  `json:"forward_origin"`
}

type Update struct {
	UpdateID int64    `json:"update_id"`
	Message  *Message `json:"message"`
}

// MediaKind 归类媒体类型，与旧版保持一致的四类。
func (m *Message) MediaKind() string {
	switch {
	case len(m.Video) > 0, len(m.VideoNote) > 0, len(m.Animation) > 0:
		return "视频"
	case len(m.Photo) > 0, len(m.Sticker) > 0:
		return "图片"
	case len(m.Document) > 0:
		return "文档"
	case len(m.Audio) > 0, len(m.Voice) > 0:
		return "音频"
	}
	return ""
}

// Source 解析转发的来源：返回可读频道名与可下载链接。
// 只有频道/群组转发才能构造出 t.me 链接 —— 私聊转发没有 message_id，无法定位。
func (m *Message) Source() (name, link string) {
	o := m.ForwardOrigin
	if o == nil {
		return "", ""
	}
	pick := o.Chat
	if pick == nil {
		pick = o.SenderChat
	}
	if pick != nil {
		name = pick.Title
		if name == "" {
			name = pick.Username
		}
	}
	if pick == nil {
		return name, ""
	}

	mid := o.MessageID
	if mid == 0 {
		mid = m.MessageID
	}
	if pick.Username != "" {
		return name, fmt.Sprintf("https://t.me/%s/%d", pick.Username, mid)
	}
	if pick.ID != 0 {
		cid := strconv.FormatInt(pick.ID, 10)
		cid = strings.TrimPrefix(cid, "-100")
		return name, fmt.Sprintf("https://t.me/c/%s/%d", cid, mid)
	}
	return name, ""
}

// ── 客户端 + 熔断 ─────────────────────────────────────────

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

// TGClient 自带熔断：连续失败会整体重建 HTTP Transport。
// 这是原 Python 版最致命的缺口 —— httpx 连接池被污染后永不恢复。
type TGClient struct {
	token string
	proxy string

	mu       sync.Mutex
	client   *http.Client
	fails    int
	backoff  time.Duration
	rebuilds int64
	lastErr  string
}

const (
	poolSize     = 32
	maxTransport = 5 // 连续失败达到这个数就重建连接池
	maxBackoff   = 60 * time.Second
)

func NewTGClient(token, proxy string) *TGClient {
	c := &TGClient{token: token, proxy: proxy}
	c.client = c.buildClient()
	return c
}

func (c *TGClient) buildClient() *http.Client {
	tr := &http.Transport{
		Proxy: func(_ *http.Request) (*url.URL, error) {
			if c.proxy == "" {
				return nil, nil
			}
			return url.Parse(c.proxy)
		},
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          poolSize * 2,
		MaxIdleConnsPerHost:   poolSize,
		MaxConnsPerHost:       0, // 不设硬上限，靠失败重建兜底
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: tr}
}

// rebuild 丢弃整个连接池。卡死的半开连接是 Pool timeout 的根源，只能整体换掉。
func (c *TGClient) rebuild() {
	if old, ok := c.client.Transport.(*http.Transport); ok {
		old.CloseIdleConnections()
	}
	c.client = c.buildClient()
	c.rebuilds++
}

func (c *TGClient) recordFailure(err error) {
	// 这里必须自己脱敏：recordFailure 在 call 的 defer 之前执行，
	// 那时 err 里还带着完整 URL（含 token），存进 lastErr 就会经 /api/stats 暴露出去
	safe := c.scrub(err)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fails++
	c.lastErr = safe.Error()
	if c.fails >= maxTransport {
		c.rebuild()
		c.fails = 0
	}
	if c.backoff == 0 {
		c.backoff = time.Second
	} else if c.backoff < maxBackoff {
		c.backoff *= 2
		if c.backoff > maxBackoff {
			c.backoff = maxBackoff
		}
	}
}

func (c *TGClient) recordSuccess() {
	c.mu.Lock()
	c.fails = 0
	c.backoff = 0
	c.lastErr = ""
	c.mu.Unlock()
}

// Backoff 当前应等待的时长（无失败时为 0）。
func (c *TGClient) Backoff() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.backoff
}

func (c *TGClient) Health() (int, int64, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fails, c.rebuilds, c.lastErr
}

func (c *TGClient) httpClient() *http.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}

// scrub 抹掉错误信息里的 bot token。
// Go 的 url.Error 会把完整请求 URL 写进 err（https://api.telegram.org/bot<token>/method），
// 一旦进日志就等于把 token 打印出来了。
func (c *TGClient) scrub(err error) error {
	if err == nil {
		return nil
	}
	s := err.Error()
	if c.token != "" && strings.Contains(s, c.token) {
		s = strings.ReplaceAll(s, c.token, "<token>")
	}
	// 兜底：任何 bot<数字>:<串> 形态也抹掉
	if i := strings.Index(s, "/bot"); i >= 0 {
		if j := strings.Index(s[i:], "/"); j >= 0 {
			rest := s[i+len("/bot"):]
			if k := strings.Index(rest, "/"); k > 0 {
				cand := rest[:k]
				if strings.Contains(cand, ":") {
					s = strings.ReplaceAll(s, cand, "<token>")
				}
			}
		}
	}
	return errors.New(s)
}

// call 统一请求入口：处理超时、429 retry_after、非 200。
// 所有错误出口都过 scrub，避免 token 进日志。
func (c *TGClient) call(ctx context.Context, method string, payload any, out any) (err error) {
	defer func() {
		if err != nil {
			err = c.scrub(err)
		}
	}()

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/%s", c.token, method)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		c.recordFailure(err)
		return err
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
	}()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.recordFailure(err)
		return err
	}

	var api apiResponse
	if err := json.Unmarshal(raw, &api); err != nil {
		c.recordFailure(err)
		return fmt.Errorf("响应解析失败: %w", err)
	}

	if !api.OK {
		// 429 必须尊重服务端给的等待时间，否则会被越限越狠
		if api.ErrorCode == 429 && api.Parameters.RetryAfter > 0 {
			return errRateLimited{wait: time.Duration(api.Parameters.RetryAfter) * time.Second,
				desc: api.Description}
		}
		// 4xx 是请求本身的问题，不该触发熔断重建
		if api.ErrorCode >= 400 && api.ErrorCode < 500 {
			return fmt.Errorf("telegram %d: %s", api.ErrorCode, api.Description)
		}
		err = fmt.Errorf("telegram %d: %s", api.ErrorCode, api.Description)
		c.recordFailure(err)
		return err
	}

	c.recordSuccess()
	if out != nil {
		if err := json.Unmarshal(api.Result, out); err != nil {
			return err
		}
	}
	return nil
}

type errRateLimited struct {
	wait time.Duration
	desc string
}

func (e errRateLimited) Error() string {
	return fmt.Sprintf("触发限流，需等待 %s（%s）", e.wait, e.desc)
}

func AsRateLimited(err error) (time.Duration, bool) {
	var r errRateLimited
	if errors.As(err, &r) {
		return r.wait, true
	}
	return 0, false
}

func (c *TGClient) GetMe(ctx context.Context) (*User, error) {
	var u User
	err := c.call(ctx, "getMe", map[string]any{}, &u)
	return &u, err
}

// GetUpdates 长轮询。timeout 是服务端挂起秒数，HTTP 超时要比它长。
func (c *TGClient) GetUpdates(ctx context.Context, offset int64, timeoutSec int, limit int) ([]Update, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	payload := map[string]any{
		"offset":          offset,
		"timeout":         timeoutSec,
		"limit":           limit,
		"allowed_updates": []string{"message"},
	}
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second+20*time.Second)
	defer cancel()

	var ups []Update
	err := c.call(reqCtx, "getUpdates", payload, &ups)
	return ups, err
}

type SendOpts struct {
	ChatID    int64
	Text      string
	ParseMode string
}

// Send 发消息，返回 message_id 供后续编辑成进度条。
func (c *TGClient) Send(ctx context.Context, o SendOpts) (int64, error) {
	payload := map[string]any{"chat_id": o.ChatID, "text": o.Text}
	if o.ParseMode != "" {
		payload["parse_mode"] = o.ParseMode
	}
	payload["disable_web_page_preview"] = true

	var m Message
	if err := c.call(ctx, "sendMessage", payload, &m); err != nil {
		return 0, err
	}
	return m.MessageID, nil
}

// Edit 编辑已有消息。内容未变化时 Telegram 会报错，这里忽略即可。
func (c *TGClient) Edit(ctx context.Context, chatID, msgID int64, text string) error {
	payload := map[string]any{
		"chat_id":                  chatID,
		"message_id":               msgID,
		"text":                     text,
		"disable_web_page_preview": true,
	}
	return c.call(ctx, "editMessageText", payload, nil)
}

func (c *TGClient) Delete(ctx context.Context, chatID, msgID int64) error {
	return c.call(ctx, "deleteMessage", map[string]any{
		"chat_id": chatID, "message_id": msgID,
	}, nil)
}
