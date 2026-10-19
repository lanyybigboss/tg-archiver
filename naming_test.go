package main

import (
	"path/filepath"
	"testing"
)

// 用的都是 NAS 上真实存在的文件名与标签。规则改了必须让这些用例继续通过。

func TestBuildFileName(t *testing.T) {
	cases := []struct {
		desc     string
		orig     string
		tag      string
		media    string
		expected string
	}{
		{
			desc: "有意义的描述保留，序号也保留",
			orig: "城市夜景延时摄影 (10).mp4", tag: "Alice", media: "视频",
			expected: "Alice_城市夜景延时摄影 (10).mp4",
		},
		{
			desc: "纯数字保留序号（避免同标签下撞名）",
			orig: "11.mp4", tag: "Bob", media: "视频",
			expected: "Bob_11.mp4",
		},
		{
			desc: "TG 文件 ID 用标签",
			orig: "5107239706277121652.jpg", tag: "Bob", media: "图片",
			expected: "Bob.jpg",
		},
		{
			desc: "trim_head 技术名用标签",
			orig: "trim_head_5e4df41b.mp4", tag: "StudioX", media: "视频",
			expected: "StudioX.mp4",
		},
		{
			desc: "相机名 IMG_ 保留序号",
			orig: "IMG_9243.MP4", tag: "Cam01", media: "视频",
			expected: "Cam01_9243.mp4",
		},
		{
			desc: "时间轴去掉但保留 seg 标识",
			orig: "DeltaStudio-00.00.00.000-00.09.53.438-seg1.mp4", tag: "Bob", media: "视频",
			expected: "Bob_DeltaStudio_seg1.mp4",
		},
		{
			desc: "推广串与下划线时间轴一起清掉",
			orig: "抖音_日常vlog_电报群@examplechannel_1_00_03_35_517_00_06_42_299.mp4", tag: "日常vlog", media: "视频",
			expected: "抖音_日常vlog_电报群_1.mp4", // 文件名已含标签，不重复加前缀
		},
		{
			desc: "中文描述原样保留",
			orig: "周末咖啡店探店记录！.mp4", tag: "Alice", media: "视频",
			expected: "Alice_周末咖啡店探店记录！.mp4",
		},
		{
			desc: "纯哈希名用标签",
			orig: "a3f5e9c1b2d4e5f6.mp4", tag: "Alice", media: "视频",
			expected: "Alice.mp4",
		},
		{
			desc: "日期名视为有意义（保留日期）",
			orig: "9月25日(2).mp4", tag: "Carol", media: "视频",
			expected: "Carol_9月25日(2).mp4",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			got := BuildFileName(c.orig, c.tag, c.media, nil, nil)
			if got != c.expected {
				t.Errorf("\n  原名: %s\n  标签: %s\n  期望: %s\n  实际: %s", c.orig, c.tag, c.expected, got)
			}
		})
	}
}

func TestBuildFileNameDedup(t *testing.T) {
	taken := map[string]bool{"Bob_1.mp4": true}
	got := BuildFileName("1.mp4", "Bob", "视频", nil, func(n string) bool { return taken[n] })
	if got != "Bob_1_2.mp4" {
		t.Errorf("重名应加序号，得到 %s", got)
	}
}

func TestIsMeaningfulName(t *testing.T) {
	meaningful := []string{"这是一个比较长的中文描述性标题", "DeltaStudio-00.00.00.000-seg1", "black stockings model", "9月25日"}
	notMeaningful := []string{"1", "(1)", "123456789012345", "trim_head_5e4df41b",
		"a3f5e9c1b2d4e5f6", "IMG_20240101", "seg1", "", "5107239706277121652"}

	for _, s := range meaningful {
		if !IsMeaningfulName(s) {
			t.Errorf("应判定为有意义: %q", s)
		}
	}
	for _, s := range notMeaningful {
		if IsMeaningfulName(s) {
			t.Errorf("应判定为无意义: %q", s)
		}
	}
}

// 作者库只认「文件名里出现」与「标签」，不看频道。
func TestAuthorLibResolve(t *testing.T) {
	lib := NewAuthorLib([]Author{
		{Name: "Alice", Aliases: "alice,A"},
		{Name: "StudioX", Aliases: "sx,studio"},
		{Name: "Bob"},
		{Name: "DeltaStudio", Aliases: "delta"},
	})

	cases := []struct {
		desc     string
		orig     string
		tag      string
		expected string
	}{
		{"文件名里出现作者名", "DeltaStudio-00.00.00.000-seg1.mp4", "Bob", "DeltaStudio"},
		{"别名匹配标签", "1.mp4", "sx", "StudioX"},
		{"标签直接就是作者", "1.mp4", "Alice", "Alice"},
		{"什么都不命中", "1.mp4", "陌生标签", ""},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			if got := lib.Resolve(c.orig, c.tag); got != c.expected {
				t.Errorf("Resolve(%q,%q) = %q, 期望 %q", c.orig, c.tag, got, c.expected)
			}
		})
	}
}

// 作者库认出别的作者时以它为准，而不是用户的标签。
func TestAuthorLibOverridesTag(t *testing.T) {
	lib := NewAuthorLib([]Author{{Name: "DeltaStudio", Aliases: "delta"}})
	got := BuildFileName("DeltaStudio-00.00.00.000-seg1.mp4", "Bob", "视频", lib, nil)
	if got != "DeltaStudio_seg1.mp4" {
		t.Errorf("应以识别出的作者命名且不重复加前缀，得到 %q", got)
	}
}

// 归档目录只有标签一层，不再有「类型」或「来源频道」子目录。
func TestArchiveDir(t *testing.T) {
	got := ArchiveDir("/opt/tg-archiver/data/downloads", "Alice")
	want := filepath.Join("/opt/tg-archiver/data/downloads", "Alice")
	if got != want {
		t.Errorf("归档目录应为 %s，得到 %s", want, got)
	}

	got = ArchiveDir("/d", "a/b:c")
	if filepath.Base(got) != "a_b_c" {
		t.Errorf("非法字符未清理，得到 %q", filepath.Base(got))
	}
}

// 别名也要参与"文件名已含作者"的判断，否则会多出冗余前缀。
func TestAliasPreventsDuplicatePrefix(t *testing.T) {
	lib := NewAuthorLib([]Author{
		{Name: "studiox", Aliases: "Studio_X"},
		{Name: "alice", Aliases: "nmdalice"},
	})

	cases := []struct{ orig, tag, want string }{
		{"Studio_X-001.mp4", "某标签", "Studio_X-001.mp4"},
		{"studiox-001.mp4", "某标签", "studiox-001.mp4"},
		{"nmdalice-01.mp4", "某标签", "nmdalice-01.mp4"},
		// 文件名里没有作者信息时，作者库不命中，回落到标签
		{"完全无关的名字.mp4", "某标签", "某标签_完全无关的名字.mp4"},
	}
	for _, c := range cases {
		got := BuildFileName(c.orig, c.tag, "视频", lib, nil)
		if got != c.want {
			t.Errorf("%s: 期望 %s，得到 %s", c.orig, c.want, got)
		}
	}
}
