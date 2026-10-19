package main

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// QueueItem 一条下载任务。
type QueueItem struct {
	ID         int64   `json:"id"`
	ChatID     int64   `json:"chat_id"`
	MsgID      int64   `json:"msg_id"`
	Tag        string  `json:"tag"`
	Channel    string  `json:"channel"`
	MediaType  string  `json:"media_type"`
	URL        string  `json:"url"`
	Status     string  `json:"status"`
	RetryCount int     `json:"retry_count"`
	Progress   float64 `json:"progress"`
	Speed      string  `json:"speed"`
	FPath      string  `json:"fpath"`
	ErrorMsg   string  `json:"error_msg"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
}

type Stats struct {
	Pending    int   `json:"pending"`
	Processing int   `json:"processing"`
	Done       int   `json:"done"`
	Failed     int   `json:"failed"`
	DiskBytes  int64 `json:"disk_bytes"`
}

type Store struct {
	db *sql.DB
}

// dbNow 与原表保持一致：SQLite CURRENT_TIMESTAMP 是 UTC。
func dbNow() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

func OpenStore(path string) (*Store, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// migrate 只做加法：旧库（Python 版写入的）可以直接沿用，不改动已有数据。
func (s *Store) migrate() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS history (
			chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL, tag TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (chat_id, msg_id)
		)`,
		`CREATE TABLE IF NOT EXISTS queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_id INTEGER NOT NULL, msg_id INTEGER NOT NULL,
			tag TEXT NOT NULL, media_type TEXT NOT NULL, url TEXT NOT NULL,
			status TEXT DEFAULT 'pending', retry_count INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS upload_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			fpath TEXT NOT NULL, fsize INTEGER, tag TEXT,
			uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS fail_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			chat_id INTEGER, msg_id INTEGER, tag TEXT, url TEXT, reason TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS state (
			key TEXT PRIMARY KEY, value TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			level TEXT NOT NULL,
			kind TEXT NOT NULL,
			message TEXT NOT NULL,
			task_id INTEGER DEFAULT 0,
			payload TEXT DEFAULT '',
			acked INTEGER DEFAULT 0,
			pushed INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS authors (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			aliases TEXT DEFAULT '',
			channel TEXT DEFAULT '',
			note TEXT DEFAULT '',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_queue_status ON queue(status)`,
	}
	for _, q := range schema {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("建表失败: %w\n%s", err, q)
		}
	}

	// 新增列，重复执行安全
	alters := []string{
		`ALTER TABLE queue ADD COLUMN channel TEXT`,
		`ALTER TABLE queue ADD COLUMN progress REAL DEFAULT 0`,
		`ALTER TABLE queue ADD COLUMN fpath TEXT`,
		`ALTER TABLE queue ADD COLUMN error_msg TEXT`,
		`ALTER TABLE queue ADD COLUMN retry_after INTEGER DEFAULT 0`,
		`ALTER TABLE queue ADD COLUMN speed TEXT`,
		`ALTER TABLE history ADD COLUMN channel TEXT`,
	}
	for _, a := range alters {
		if _, err := s.db.Exec(a); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return fmt.Errorf("加列失败: %w\n%s", err, a)
		}
	}
	return nil
}

// ResetStale 把上次进程遗留的 processing 任务放回 pending。
// 原 Python 版缺这一步 —— 崩溃/重启后这些任务永远僵死，这是"入了队却永远不动"的元凶。
func (s *Store) ResetStale() (int64, error) {
	res, err := s.db.Exec(`UPDATE queue SET status='pending', updated_at=? WHERE status='processing'`, dbNow())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) IsDuplicate(chatID, msgID int64) bool {
	var x int
	err := s.db.QueryRow(`SELECT 1 FROM history WHERE chat_id=? AND msg_id=?`, chatID, msgID).Scan(&x)
	return err == nil
}

func (s *Store) AddHistory(chatID, msgID int64, tag, channel string) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO history (chat_id, msg_id, tag, channel) VALUES (?,?,?,?)`,
		chatID, msgID, tag, channel)
	return err
}

func (s *Store) Enqueue(chatID, msgID int64, tag, channel, mediaType, url string) (int64, error) {
	now := dbNow()
	res, err := s.db.Exec(
		`INSERT INTO queue (chat_id,msg_id,tag,channel,media_type,url,status,created_at,updated_at)
		 VALUES (?,?,?,?,?,?,'pending',?,?)`,
		chatID, msgID, tag, channel, mediaType, url, now, now)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Claim 原子取任务：UPDATE ... RETURNING 保证多 worker 并发下不会重复消费同一条。
// retry_after 让失败任务退避期间不被立刻重取（原版用 sleep 占着协程，会拖死并发）。
func (s *Store) Claim(n int) ([]QueueItem, error) {
	if n < 1 {
		return nil, nil
	}
	rows, err := s.db.Query(`
		UPDATE queue SET status='processing', updated_at=?
		WHERE id IN (
			SELECT id FROM queue WHERE status='pending' AND COALESCE(retry_after,0) <= ?
			ORDER BY id LIMIT ?
		)
		RETURNING id, chat_id, msg_id, tag, COALESCE(channel,''), media_type, url,
		          status, retry_count, COALESCE(progress,0), COALESCE(speed,''),
		          COALESCE(fpath,''), COALESCE(error_msg,''),
		          COALESCE(created_at,''), COALESCE(updated_at,'')`, dbNow(), time.Now().Unix(), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []QueueItem
	for rows.Next() {
		var it QueueItem
		if err := rows.Scan(&it.ID, &it.ChatID, &it.MsgID, &it.Tag, &it.Channel, &it.MediaType,
			&it.URL, &it.Status, &it.RetryCount, &it.Progress, &it.Speed, &it.FPath, &it.ErrorMsg,
			&it.CreatedAt, &it.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// SetProgress 更新百分比。speed 为空时不动速度列 ——
// tdl 的进度行没有速度段，若照样写空串，会把 trackSpeed 实测出来的速度冲掉。
func (s *Store) SetProgress(id int64, pct float64, speed string) {
	if speed != "" {
		s.db.Exec(`UPDATE queue SET progress=?, speed=?, updated_at=? WHERE id=?`, pct, speed, dbNow(), id)
		return
	}
	s.db.Exec(`UPDATE queue SET progress=?, updated_at=? WHERE id=?`, pct, dbNow(), id)
}

// SetSpeed 只更新速度列（由文件增长速率算出，与 tdl 输出无关），
// 和 SetProgress 改的是不同列，不会互相覆盖。
func (s *Store) SetSpeed(id int64, speed string) {
	s.db.Exec(`UPDATE queue SET speed=? WHERE id=?`, speed, id)
}

func (s *Store) MarkDone(id int64, fpath string, size int64, tag string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE queue SET status='done', progress=100, speed='', fpath=?, updated_at=? WHERE id=?`,
		fpath, dbNow(), id); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO upload_log (fpath, fsize, tag) VALUES (?,?,?)`, fpath, size, tag); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkFailed(id int64, chatID, msgID int64, tag, url, reason string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE queue SET status='failed', error_msg=?, updated_at=? WHERE id=?`,
		reason, dbNow(), id); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO fail_log (chat_id,msg_id,tag,url,reason) VALUES (?,?,?,?,?)`,
		chatID, msgID, tag, url, reason); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkRetry(id int64, reason string, delay time.Duration) error {
	_, err := s.db.Exec(
		`UPDATE queue SET status='pending', retry_count=retry_count+1, error_msg=?,
		 retry_after=?, updated_at=? WHERE id=?`,
		reason, time.Now().Add(delay).Unix(), dbNow(), id)
	return err
}

// Requeue 把任务放回队列但不计失败次数。
// 服务重启/优雅退出时正在下载的任务会走这条路径 —— 那不是任务的错，不该消耗重试额度。
func (s *Store) Requeue(id int64, reason string) error {
	_, err := s.db.Exec(
		`UPDATE queue SET status='pending', retry_after=0, error_msg=?, updated_at=? WHERE id=?`,
		reason, dbNow(), id)
	return err
}

// SetStatus 供面板手动操作：retry 把任务放回队列，cancel 置为 failed。
func (s *Store) SetStatus(id int64, status string) error {
	if status == "pending" {
		_, err := s.db.Exec(
			`UPDATE queue SET status='pending', retry_count=0, retry_after=0, error_msg='', progress=0, updated_at=? WHERE id=?`,
			dbNow(), id)
		return err
	}
	_, err := s.db.Exec(`UPDATE queue SET status=?, updated_at=? WHERE id=?`, status, dbNow(), id)
	return err
}

func (s *Store) Get(id int64) (*QueueItem, error) {
	var it QueueItem
	err := s.db.QueryRow(`SELECT id, chat_id, msg_id, tag, COALESCE(channel,''), media_type, url,
		status, retry_count, COALESCE(progress,0), COALESCE(speed,''), COALESCE(fpath,''), COALESCE(error_msg,''),
		COALESCE(created_at,''), COALESCE(updated_at,'') FROM queue WHERE id=?`, id).
		Scan(&it.ID, &it.ChatID, &it.MsgID, &it.Tag, &it.Channel, &it.MediaType, &it.URL,
			&it.Status, &it.RetryCount, &it.Progress, &it.Speed, &it.FPath, &it.ErrorMsg, &it.CreatedAt, &it.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &it, nil
}

func (s *Store) ListQueue(status string, limit int) ([]QueueItem, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	q := `SELECT id, chat_id, msg_id, tag, COALESCE(channel,''), media_type, url,
		status, retry_count, COALESCE(progress,0), COALESCE(speed,''), COALESCE(fpath,''), COALESCE(error_msg,''),
		COALESCE(created_at,''), COALESCE(updated_at,'') FROM queue`
	args := []any{}
	if status != "" && status != "all" {
		q += ` WHERE status=?`
		args = append(args, status)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []QueueItem{}
	for rows.Next() {
		var it QueueItem
		if err := rows.Scan(&it.ID, &it.ChatID, &it.MsgID, &it.Tag, &it.Channel, &it.MediaType,
			&it.URL, &it.Status, &it.RetryCount, &it.Progress, &it.Speed, &it.FPath, &it.ErrorMsg,
			&it.CreatedAt, &it.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (s *Store) Stats() (*Stats, error) {
	st := &Stats{}
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM queue GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		switch status {
		case "pending":
			st.Pending = n
		case "processing":
			st.Processing = n
		case "done":
			st.Done = n
		case "failed":
			st.Failed = n
		}
	}
	return st, rows.Err()
}

// DoneForCleanup 按更新时间从旧到新返回已完成任务，供缓存清理。
func (s *Store) DoneForCleanup() ([]QueueItem, error) {
	rows, err := s.db.Query(`SELECT id, COALESCE(fpath,''), COALESCE(updated_at,'')
		FROM queue WHERE status='done' AND COALESCE(fpath,'')<>'' ORDER BY updated_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueItem
	for rows.Next() {
		var it QueueItem
		if err := rows.Scan(&it.ID, &it.FPath, &it.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (s *Store) GetState(key, def string) string {
	var v string
	if err := s.db.QueryRow(`SELECT value FROM state WHERE key=?`, key).Scan(&v); err != nil {
		return def
	}
	return v
}

func (s *Store) SetState(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO state(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

// ── 作者库 ────────────────────────────────────────────────
// 用途：① 别名归一（StudioX / studiox / sx 视为同一人）
//      ② 从文件名里认出作者（DeltaStudio-00.00.00.mp4 → 作者 DeltaStudio）
//      ③ 从文件名里认出作者（DeltaStudio-00.00.00.mp4 → 作者 DeltaStudio）

type Author struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Aliases   string `json:"aliases"`
	Channel   string `json:"channel"`
	Note      string `json:"note"`
	CreatedAt string `json:"created_at"`
}

func splitAliases(s string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '，' || r == '/' || r == ';' || r == '；'
	}) {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (s *Store) ListAuthors() ([]Author, error) {
	rows, err := s.db.Query(`SELECT id, name, COALESCE(aliases,''), COALESCE(channel,''),
		COALESCE(note,''), COALESCE(created_at,'') FROM authors ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Author{}
	for rows.Next() {
		var a Author
		if err := rows.Scan(&a.ID, &a.Name, &a.Aliases, &a.Channel, &a.Note, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) AddAuthor(name, aliases, channel, note string) (int64, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, fmt.Errorf("作者名不能为空")
	}
	res, err := s.db.Exec(`INSERT OR IGNORE INTO authors (name, aliases, channel, note) VALUES (?,?,?,?)`,
		name, strings.TrimSpace(aliases), strings.TrimSpace(channel), strings.TrimSpace(note))
	if err != nil {
		return 0, err
	}
	// 注意：INSERT OR IGNORE 冲突时 LastInsertId() 返回的是上一次成功插入的 rowid，
	// 不是 0。必须用 RowsAffected 判断，否则会把"已存在"错算成"新增"。
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil // 已存在
	}
	return res.LastInsertId()
}

func (s *Store) UpdateAuthor(id int64, name, aliases, channel, note string) error {
	_, err := s.db.Exec(`UPDATE authors SET name=?, aliases=?, channel=?, note=? WHERE id=?`,
		strings.TrimSpace(name), strings.TrimSpace(aliases), strings.TrimSpace(channel), strings.TrimSpace(note), id)
	return err
}

func (s *Store) DeleteAuthor(id int64) error {
	_, err := s.db.Exec(`DELETE FROM authors WHERE id=?`, id)
	return err
}

// SeedAuthorsFromTags 首次运行时用历史标签做种子。
// 用户的标签本来就是他手动分类的博主名，是最干净的一份作者名单。
func (s *Store) SeedAuthorsFromTags() (int, error) {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM authors`).Scan(&n); err != nil {
		return 0, err
	}
	if n > 0 {
		return 0, nil
	}
	res, err := s.db.Exec(`INSERT OR IGNORE INTO authors (name, note)
		SELECT DISTINCT tag, '来自历史标签' FROM queue
		WHERE tag IS NOT NULL AND tag <> '' AND tag <> '未分类'`)
	if err != nil {
		return 0, err
	}
	affected, _ := res.RowsAffected()
	return int(affected), nil
}

// SeedAuthorsFromTagsForce 把当前所有标签里还没入库的补进来。
// 与首次种子不同，这个可以反复调用，用于「后来新增了标签」的场景。
func (s *Store) SeedAuthorsFromTagsForce() (int, error) {
	res, err := s.db.Exec(`INSERT OR IGNORE INTO authors (name, note)
		SELECT DISTINCT tag, '同步自标签' FROM queue
		WHERE tag IS NOT NULL AND tag <> '' AND tag <> '未分类'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ── 事件（给 agent 消费：报错、节点切换、磁盘告警等）────────

type Event struct {
	ID        int64  `json:"id"`
	Level     string `json:"level"`
	Kind      string `json:"kind"`
	Message   string `json:"message"`
	TaskID    int64  `json:"task_id"`
	Payload   string `json:"payload"`
	Acked     int    `json:"acked"`
	Pushed    int    `json:"pushed"`
	CreatedAt string `json:"created_at"`
}

func (s *Store) AddEvent(level, kind, message string, taskID int64, payload string) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO events (level,kind,message,task_id,payload) VALUES (?,?,?,?,?)`,
		level, kind, message, taskID, payload)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) MarkEventPushed(id int64) {
	s.db.Exec(`UPDATE events SET pushed=1 WHERE id=?`, id)
}

func (s *Store) ListEvents(sinceID int64, limit int, unackedOnly bool) ([]Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT id, level, kind, message, task_id, COALESCE(payload,''), acked, pushed,
		COALESCE(created_at,'') FROM events WHERE id > ?`
	args := []any{sinceID}
	if unackedOnly {
		q += ` AND acked = 0`
	}
	q += ` ORDER BY id ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Level, &e.Kind, &e.Message, &e.TaskID,
			&e.Payload, &e.Acked, &e.Pushed, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) AckEvent(id int64) error {
	res, err := s.db.Exec(`UPDATE events SET acked=1 WHERE id=?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("事件 %d 不存在", id)
	}
	return nil
}

func (s *Store) AckAllEvents() (int64, error) {
	res, err := s.db.Exec(`UPDATE events SET acked=1 WHERE acked=0`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// UnackedEventCount 供面板显示红点。
func (s *Store) UnackedEventCount() int {
	var n int
	s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE acked=0`).Scan(&n)
	return n
}
