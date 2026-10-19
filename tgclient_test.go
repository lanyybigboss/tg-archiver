package main

import "testing"

// Source 决定了一条转发能不能入队。这是本工具的使用边界所在，
// 行为一旦被改动必须有测试拦住。
func TestSource(t *testing.T) {
	t.Run("公开频道应放行", func(t *testing.T) {
		m := &Message{
			MessageID: 500,
			ForwardOrigin: &ForwardOrigin{
				Chat:      &Chat{ID: -1001111111111, Username: "publicchannel", Title: "公开频道"},
				MessageID: 42,
			},
		}
		name, link, isPublic := m.Source()
		if !isPublic {
			t.Fatal("公开频道应判定为 isPublic=true")
		}
		if name != "公开频道" {
			t.Errorf("频道名 = %q，期望 %q", name, "公开频道")
		}
		if want := "https://t.me/publicchannel/42"; link != want {
			t.Errorf("链接 = %q，期望 %q", link, want)
		}
	})

	t.Run("私密频道必须拒绝", func(t *testing.T) {
		m := &Message{
			MessageID: 501,
			ForwardOrigin: &ForwardOrigin{
				Chat:      &Chat{ID: -1002222222222, Title: "私密群"},
				MessageID: 43,
			},
		}
		name, link, isPublic := m.Source()
		if isPublic {
			t.Error("无 @username 的来源不应判定为公开")
		}
		if link != "" {
			t.Errorf("私密来源不应生成链接，却得到 %q", link)
		}
		if name != "私密群" {
			t.Errorf("仍应返回频道名用于提示，得到 %q", name)
		}
	})

	t.Run("私聊转发无法定位", func(t *testing.T) {
		m := &Message{MessageID: 502}
		name, link, isPublic := m.Source()
		if isPublic || link != "" || name != "" {
			t.Errorf("私聊转发应全部为空，得到 name=%q link=%q public=%v", name, link, isPublic)
		}
	})

	t.Run("sender_chat 来源同样适用", func(t *testing.T) {
		m := &Message{
			MessageID: 503,
			ForwardOrigin: &ForwardOrigin{
				SenderChat: &Chat{ID: -1003333333333, Username: "groupchat", Title: "群组"},
				MessageID:  44,
			},
		}
		_, link, isPublic := m.Source()
		if !isPublic || link != "https://t.me/groupchat/44" {
			t.Errorf("sender_chat 公开来源应放行，得到 link=%q public=%v", link, isPublic)
		}
	})
}
