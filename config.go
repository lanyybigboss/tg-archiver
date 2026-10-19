package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config 沿用原 Python 版的 .env 变量名，可直接复用 NAS 上已有的 .env 文件。
type Config struct {
	BotToken     string
	OwnerID      int64
	BaseDir      string
	TDLBinary    string
	TDLProxy     string
	HTTPProxy    string
	CacheLimitGB int64
	MaxWorkers   int
	TDLThreads   int
	TDLTimeout   time.Duration
	MaxRetry     int
	PollTimeout  int
	PanelAddr    string
	PanelToken   string
	PanelURL     string

	// 告警：给 NAS 上的 agent（AstrBot）用
	AlertWebhook       string
	AlertWebhookFormat string
	// 节点测速
	NodeTestEnabled  bool
	NodeTestInterval time.Duration
	MihomoAPI        string
	NodeGroup        string

	DownloadDir string
	WorkerDir   string
	DBPath      string
	LogDir      string
}

// loadDotEnv 极简 .env 解析：只支持 KEY=VALUE，忽略注释与空行，容忍引号。
// 已存在的环境变量优先，不会被文件覆盖。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"'`)
		if k == "" {
			continue
		}
		if _, exists := os.LookupEnv(k); exists {
			continue
		}
		os.Setenv(k, v)
	}
}

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func loadConfig() (*Config, error) {
	// 先定位 BASE_DIR，再决定去哪读 .env
	baseDir := envStr("BASE_DIR", "/opt/tg-archiver/data")
	loadDotEnv(filepath.Join(baseDir, ".env"))
	// .env 里可能改写 BASE_DIR，重读一次
	baseDir = envStr("BASE_DIR", baseDir)

	cfg := &Config{
		BotToken:     envStr("BOT_TOKEN", ""),
		OwnerID:      envInt64("OWNER_ID", 0),
		BaseDir:      baseDir,
		TDLBinary:    envStr("TDL_BINARY", "tdl"),
		TDLProxy:     envStr("TDL_PROXY", "socks5://127.0.0.1:7891"),
		HTTPProxy:    envStr("HTTP_PROXY", "http://127.0.0.1:7890"),
		CacheLimitGB: envInt64("CACHE_LIMIT_GB", 80),
		MaxWorkers:   envInt("MAX_WORKERS", 2),
		TDLThreads:   envInt("TDL_THREADS", 16),
		TDLTimeout:   time.Duration(envInt("TDL_TIMEOUT", 900)) * time.Second,
		MaxRetry:     envInt("MAX_RETRY", 10),
		PollTimeout:  envInt("POLL_TIMEOUT", 30),
		// 绑 0.0.0.0 才能让本机 curl/健康检查访问；外网访问由 iptables 挡（INPUT 默认 DROP，只放行内网段）
		PanelAddr:  envStr("PANEL_ADDR", "0.0.0.0:6186"),
		PanelToken: envStr("PANEL_TOKEN", ""),
		PanelURL:   envStr("PANEL_URL", "http://127.0.0.1:6186/"),

		AlertWebhook:       envStr("ALERT_WEBHOOK", ""),
		AlertWebhookFormat: envStr("ALERT_WEBHOOK_FORMAT", "generic"),
		NodeTestEnabled:    envStr("NODE_TEST_ENABLED", "true") != "false",
		NodeTestInterval:   time.Duration(envInt("NODE_TEST_INTERVAL_HOURS", 3)) * time.Hour,
		MihomoAPI:          envStr("MIHOMO_API", "http://127.0.0.1:9090"),
		NodeGroup:          envStr("NODE_GROUP", "节点选择"),
	}
	cfg.DownloadDir = filepath.Join(baseDir, "downloads")
	cfg.WorkerDir = filepath.Join(baseDir, ".tdl_workers")
	cfg.DBPath = filepath.Join(baseDir, "history.db")
	cfg.LogDir = filepath.Join(baseDir, "logs")

	if cfg.BotToken == "" {
		return nil, fmt.Errorf("BOT_TOKEN 未设置（检查 %s/.env）", baseDir)
	}
	if cfg.MaxWorkers < 1 {
		cfg.MaxWorkers = 1
	}
	if cfg.MaxWorkers > 8 {
		cfg.MaxWorkers = 8
	}
	if cfg.PanelToken == "" {
		return nil, fmt.Errorf("PANEL_TOKEN 必须设置：面板可修改队列、触发清理、读取事件，" +
			"不应以无认证方式暴露。生成方式：openssl rand -hex 16")
	}
	return cfg, nil
}
