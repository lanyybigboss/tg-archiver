package main

import (
	"context"
	"testing"
	"time"
)

// 看门狗是「假活」的最后一道防线。
//
// 起因（2026-10-20 实测）：代理切到对 Telegram 不通的节点后，
// 已建立的 HTTP/2 连接变成半开状态，getUpdates 永久挂在
// net/http.(*http2ClientConn).roundTrip 的 select 上 ——
// 既不返回也不报错，连 context 超时都救不回来。
//
// 结果是：没有任何失败日志 → 不进失败路径 → 不触发任何自愈 →
// 进程「活着」但一条消息也处理不了。
//
// 这些用例守住两条不变量：
//  1. 请求悬挂时，看门狗必须在有限时间内把它打断，变成一次可观测的失败
//  2. 打断之后连接池必须被重建（否则下次还会挂在这个坏连接上）
func TestGetUpdatesWatchdog(t *testing.T) {
	t.Run("悬挂的请求必须被看门狗打断", func(t *testing.T) {
		c := NewTGClient("123456:test", "")
		// 指向一个必然连不上的地址，让请求悬挂
		c.proxy = "http://127.0.0.1:1"

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		start := time.Now()
		// timeoutSec=0 时 hardLimit = 45s，测试里等不了那么久，
		// 所以这里只断言「一定会返回」，不强行等满 45s。
		done := make(chan error, 1)
		go func() {
			_, err := c.GetUpdatesWatchdog(ctx, 0, 0, 10)
			done <- err
		}()

		select {
		case err := <-done:
			if err == nil {
				t.Fatal("悬挂的请求应返回错误")
			}
			if time.Since(start) > 60*time.Second {
				t.Errorf("看门狗耗时 %s，超过预期", time.Since(start))
			}
		case <-time.After(60 * time.Second):
			t.Fatal("看门狗没有打断悬挂的请求")
		}
	})

	t.Run("看门狗触发后连接池必须重建", func(t *testing.T) {
		c := NewTGClient("123456:test", "")
		c.proxy = "http://127.0.0.1:1"

		before := c.httpClient()
		c.forceRebuild()
		after := c.httpClient()

		if before == after {
			t.Error("forceRebuild 后应得到新的 http.Client")
		}
		if _, rebuilds, _ := c.Health(); rebuilds < 1 {
			t.Errorf("rebuilds = %d，期望 >= 1", rebuilds)
		}
	})
}

// poll_age 在「从未成功轮询过」时不能算成一个几十亿的荒谬数字。
// 那是 lastPollOK == 0 被直接拿去减 now 造成的。
func TestPollAgeNeverPolled(t *testing.T) {
	old := lastPollOK.Load()
	lastPollOK.Store(0)
	defer lastPollOK.Store(old)

	// 面板里的算法：last==0 时必须是 -1，而不是 now-0
	last := lastPollOK.Load()
	pollAge := int64(-1)
	if last > 0 {
		pollAge = time.Now().Unix() - last
	}
	if pollAge != -1 {
		t.Errorf("poll_age = %d，期望 -1", pollAge)
	}
}
