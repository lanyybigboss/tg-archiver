package main

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"
)

// 命名规则：作者_描述.ext
// - 原文件名有意义 → 清洗后保留（去推广串、时间轴、哈希）
// - 原文件名无意义（1.mp4 / 纯数字ID / trim_head_xxx）→ 用标签顶替
// - 作者取来源频道名；若描述里已包含作者则不重复加

var (
	// 末尾序号：(10) （3） [2] _4 —— 这类要保留，用于区分同系列的多个文件
	reTrailingIndex = regexp.MustCompile(`[\s_\-]*[\(（\[【]\s*\d{1,3}\s*[\)）\]】]\s*$`)
	rePureDigits    = regexp.MustCompile(`^\d+$`)
	reHexHash       = regexp.MustCompile(`^[0-9a-fA-F]{12,}$`)
	reTechName      = regexp.MustCompile(`(?i)^(trim_head_|trim_|video_|vid_|img_|image_|photo_|file_|download_|unnamed|untitled|result_|output_)`)
	// 只吃 @ 后紧接的账号名；不能用 \w，那会把 _1 这类序号一起吞掉
	rePromo         = regexp.MustCompile(`@[A-Za-z0-9]{3,32}`)                       // 频道推广串 @examplechannel
	reTimecode      = regexp.MustCompile(`\d{2}[.:_]\d{2}[.:_]\d{2}(?:[.:_]\d{1,3})?`) // 00.00.00.000 / 00:03:35:517 / 00_03_35_517
	reSegSuffix     = regexp.MustCompile(`(?i)[-_]?seg\d+$`)                        // seg1 / -seg2
	reSepRun        = regexp.MustCompile(`[\s_\-]{2,}`)
	reEdgeSep       = regexp.MustCompile(`^[\s_\-.]+|[\s_\-.]+$`)
)

// seqOf 判断文件名是否「就是一个序号」，是则返回它。
// 只认 11.mp4 / (1).mp4 / IMG_9243 这种整串即序号的，
// 不能从 trim_head_5e4df41b、a3f5e9c1b2d4 这类哈希里抠数字 —— 那是噪声不是序号。
func seqOf(stem string) string {
	s := reTrailingIndex.ReplaceAllString(stem, "")
	s = reTechName.ReplaceAllString(s, "") // 去掉 IMG_ / VID_ / trim_head_ 等已知前缀
	s = strings.Trim(s, " ()（）[]【】_-.")
	if s == "" || len(s) > 6 {
		return ""
	}
	if !rePureDigits.MatchString(s) {
		return ""
	}
	return s
}

// IsMeaningfulName 判断去掉扩展名后的文件名是否含可读信息。
func IsMeaningfulName(stem string) bool {
	s := strings.TrimSpace(stem)
	if s == "" {
		return false
	}
	// 去掉纯序号后缀后再判断主体
	core := strings.TrimSpace(reTrailingIndex.ReplaceAllString(s, ""))
	if core == "" {
		return false
	}
	if rePureDigits.MatchString(core) || reHexHash.MatchString(core) || reTechName.MatchString(core) {
		return false
	}
	// 只剩 seg 编号（如 "seg1"）也算无意义
	if only := strings.TrimSpace(reSegSuffix.ReplaceAllString(core, "")); only == "" {
		return false
	}

	han, letter := 0, 0
	for _, r := range core {
		switch {
		case unicode.Is(unicode.Han, r):
			han++
		case unicode.IsLetter(r):
			letter++
		}
	}
	// 至少 2 个汉字，或 3 个字母，才算"有内容"
	return han >= 2 || letter >= 3
}

// CleanStem 去掉噪声，保留可读部分。
func CleanStem(s string) string {
	s = rePromo.ReplaceAllString(s, "")     // @推广
	s = reTimecode.ReplaceAllString(s, "")  // 时间轴
	s = reSepRun.ReplaceAllString(s, "_")
	s = reEdgeSep.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// 清理后可能只剩分隔符，再判一次
	hasText := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.Is(unicode.Han, r) || unicode.IsDigit(r) {
			hasText = true
			break
		}
	}
	if !hasText {
		return ""
	}
	return s
}

// AuthorLib 作者库：别名归一 + 频道映射 + 从文件名认人。
type AuthorLib struct {
	authors []Author
}

func NewAuthorLib(list []Author) *AuthorLib {
	return &AuthorLib{authors: list}
}

func (l *AuthorLib) Len() int {
	if l == nil {
		return 0
	}
	return len(l.authors)
}

// Resolve 返回规范作者名，空串表示没认出来。
// 优先级：文件名里出现 > 标签匹配。**不看频道** —— 标签本来就是用户手动
// 分类的博主名，频道只是转发来源，聚合频道里混着多个作者，用它反而会认错人。
func (l *AuthorLib) Resolve(origName, tag string) string {
	if l == nil || len(l.authors) == 0 {
		return ""
	}
	nameLower := strings.ToLower(origName)

	// 1) 文件名里出现，取最长匹配（避免短别名抢走长名字）
	best, bestLen := "", 0
	for _, a := range l.authors {
		for _, cand := range append([]string{a.Name}, splitAliases(a.Aliases)...) {
			c := strings.ToLower(strings.TrimSpace(cand))
			if len([]rune(c)) < 2 {
				continue
			}
			if strings.Contains(nameLower, c) && len([]rune(c)) > bestLen {
				best, bestLen = a.Name, len([]rune(c))
			}
		}
	}
	if best != "" {
		return best
	}

	// 2) 标签
	for _, a := range l.authors {
		if strings.EqualFold(a.Name, tag) {
			return a.Name
		}
		for _, cand := range splitAliases(a.Aliases) {
			if strings.EqualFold(strings.TrimSpace(cand), tag) {
				return a.Name
			}
		}
	}
	return ""
}

// aliasesOf 返回某个规范名对应的所有别名。
func (l *AuthorLib) aliasesOf(canonical string) []string {
	if l == nil {
		return nil
	}
	for _, a := range l.authors {
		if strings.EqualFold(a.Name, canonical) {
			return splitAliases(a.Aliases)
		}
	}
	return nil
}

// containsAuthor 判断文件名里是否已经含有该作者（规范名或任一别名）。
// 只看规范名会漏掉「Studio_X 已含在文件名里但规范名是 studiox」这种情况，
// 结果是多出一层冗余前缀 studiox_Studio_X-001.mp4。
func containsAuthor(name, author string, lib *AuthorLib) bool {
	lower := strings.ToLower(name)
	if author == "" {
		return false
	}
	if strings.Contains(lower, strings.ToLower(author)) {
		return true
	}
	for _, al := range lib.aliasesOf(author) {
		if len([]rune(al)) >= 2 && strings.Contains(lower, strings.ToLower(al)) {
			return true
		}
	}
	return false
}

// BuildFileName 生成归档文件名。
// exists 用于检测重名，传 nil 表示不检查。
func BuildFileName(origName, tag, mediaType string, lib *AuthorLib, exists func(string) bool) string {
	ext := strings.ToLower(filepath.Ext(origName))
	if ext == "" {
		ext = extForMedia(mediaType)
	}
	stem := strings.TrimSuffix(origName, filepath.Ext(origName))

	var core string
	if IsMeaningfulName(stem) {
		core = CleanStem(stem)
	}
	if core == "" {
		// 无意义：用标签顶上。
		// 但如果是 11.mp4 / IMG_9243 这类带序号的，保留序号，
		// 否则同一标签下多个文件会全撞成「Bob.mp4」。
		if seq := seqOf(stem); seq != "" {
			core = SanitizeName(tag) + "_" + seq
		} else {
			core = SanitizeName(tag)
		}
	}

	// 定作者：作者库认出来就用它，否则直接用标签。
	// 标签就是用户手动分类的作者名，不需要再参考转发频道。
	author := ""
	if lib != nil {
		author = lib.Resolve(stem, tag)
	}
	if author == "" {
		author = strings.TrimSpace(tag)
	}

	if author != "" && author != unknownSource {
		author = SanitizeName(author)
		if !containsAuthor(core, author, lib) {
			core = author + "_" + core
		}
	}

	core = SanitizeName(core)
	core = clampBytes(core, 90)

	name := core + ext
	if exists == nil {
		return name
	}
	for i := 2; exists(name); i++ {
		suffix := fmt.Sprintf("_%d", i)
		name = clampBytes(core, 90-len(suffix)) + suffix + ext
		if i > 999 {
			break
		}
	}
	return name
}

func extForMedia(mediaType string) string {
	switch mediaType {
	case "视频":
		return ".mp4"
	case "图片":
		return ".jpg"
	case "音频":
		return ".m4a"
	default:
		return ".bin"
	}
}

// clampBytes 按字节截断但不切坏多字节字符。
func clampBytes(s string, max int) string {
	if len([]byte(s)) <= max {
		return s
	}
	r := []rune(s)
	for i := len(r); i > 0; i-- {
		if len([]byte(string(r[:i]))) <= max {
			return strings.TrimSpace(string(r[:i]))
		}
	}
	return ""
}
