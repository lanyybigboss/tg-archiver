package main

import (
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const unknownSource = "未知来源"

var illegalNameChars = strings.NewReplacer(
	"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_",
	"\"", "_", "<", "_", ">", "_", "|", "_", "\x00", "",
)

// SanitizeName 把标签/频道名/文件名里不能进文件系统的字符换掉。
// 同时防住超长名（部分文件系统单段上限 255 字节，中文按 3 字节算）。
func SanitizeName(s string) string {
	s = strings.TrimSpace(s)
	s = illegalNameChars.Replace(s)
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, s)
	s = strings.Trim(s, " .")
	if s == "" {
		return "未命名"
	}
	if len([]byte(s)) > 120 {
		r := []rune(s)
		cut := 0
		for i := range r {
			if len([]byte(string(r[:i+1]))) > 120 {
				cut = i
				break
			}
		}
		if cut > 0 {
			r = r[:cut]
		}
		s = strings.TrimSpace(string(r))
		if s == "" {
			s = "未命名"
		}
	}
	return s
}

// ArchiveDir 归档路径：downloads/标签/
// 标签本身就是作者，不再按「类型」或「来源频道」分层 —— 频道只是转发来源，
// 聚合频道里混着多个作者，按它分目录会把同一个人拆散。
func ArchiveDir(downloadDir, tag string) string {
	return filepath.Join(downloadDir, SanitizeName(tag))
}

type FileMeta struct {
	Size    int64
	ModTime time.Time
}

func isTempFile(name string) bool {
	l := strings.ToLower(name)
	return strings.HasSuffix(l, ".tmp") || strings.HasSuffix(l, ".tdl") ||
		strings.HasSuffix(l, ".part") || strings.HasSuffix(l, ".download")
}

// Snapshot 记录目录下现有文件，用于下载前后比对，只认真正新增/变动的文件。
// 旧版无脑遍历整个目录，会把历史文件重复记一遍。
func Snapshot(dir string) map[string]FileMeta {
	out := map[string]FileMeta{}
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if isTempFile(path) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		out[path] = FileMeta{Size: info.Size(), ModTime: info.ModTime()}
		return nil
	})
	return out
}

// NewFiles 返回本次下载真正产生/更新的文件。
func NewFiles(before, after map[string]FileMeta) []string {
	var out []string
	for p, a := range after {
		b, existed := before[p]
		if !existed || b.Size != a.Size || !b.ModTime.Equal(a.ModTime) {
			out = append(out, p)
		}
	}
	return out
}

func ListMediaFiles(dir string) []string {
	var out []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() || isTempFile(e.Name()) {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out
}

func DirSize(dir string) int64 {
	var total int64
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}

// CleanupCache 缓存超限时，按完成时间从旧到新删已归档文件。
// 旧版靠 upload_log 判断是否已上传，但那张表没有唯一约束、INSERT OR IGNORE 形同虚设，
// 清理逻辑长期是失效的。这里直接以队列的 done 记录为准。
func CleanupCache(cfg *Config, st *Store) {
	limit := cfg.CacheLimitGB * 1024 * 1024 * 1024
	if limit <= 0 {
		return
	}
	total := DirSize(cfg.DownloadDir)
	if total <= limit {
		return
	}
	target := int64(float64(limit) * 0.9)
	need := total - target

	items, err := st.DoneForCleanup()
	if err != nil {
		slog.Warn("清理缓存：读取完成记录失败", "err", err)
		return
	}

	var freed int64
	for _, it := range items {
		if freed >= need {
			break
		}
		for _, p := range strings.Split(it.FPath, "|") {
			p = strings.TrimSpace(p)
			if p == "" || !strings.HasPrefix(p, cfg.DownloadDir) {
				continue
			}
			info, err := os.Stat(p)
			if err != nil || info.IsDir() {
				continue
			}
			if err := os.Remove(p); err != nil {
				slog.Warn("删除缓存失败", "path", p, "err", err)
				continue
			}
			freed += info.Size()
			slog.Info("缓存清理", "file", filepath.Base(p), "mb", fmt.Sprintf("%.1f", float64(info.Size())/1024/1024))
		}
	}
	if freed > 0 {
		slog.Info("缓存清理完成", "freed_gb", fmt.Sprintf("%.2f", float64(freed)/1024/1024/1024))
	}
}
