# tg-archiver

[![CI](https://github.com/lanyybigboss/tg-archiver/actions/workflows/ci.yml/badge.svg)](https://github.com/lanyybigboss/tg-archiver/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-1.23-00ADD8.svg)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

把 Telegram 频道/群组里转发过来的媒体，按标签归档到本地 NAS。

单二进制、零运行时依赖、内置 Web 面板。用 Go 写调度层，用 [`tdl`](https://github.com/iyear/tdl) 做实际下载。

```
Telegram Bot API ──长轮询──> poller ──> SQLite 队列 ──> worker pool ──> tdl ──> 本地目录
                                            ↑                              │
                                       Web 面板                    进度回写 / 事件上报
```

## 使用边界与责任

本工具用于**个人归档你有权保存的 Telegram 内容**。

- 工具本身**不判断**内容是否有版权、是否允许保存。使用者需自行确认。
- 工具依赖 [`tdl`](https://github.com/iyear/tdl) 完成实际下载。tdl 支持从受保护的聊天中下载内容，
  使用者应遵守相关频道的规则及所在地法律。
- 请勿将归档内容公开分发或用于商业用途，除非你已获得明确授权。

写好这段话不够——**文档说的边界，代码要真的守住**。所以本工具在实现上做了三件事：

| 约束 | 实现 |
|---|---|
| **只接受公开频道** | 转发来源必须有 `@username`。没有的话无法构造 `t.me/<name>/<id>` 链接，直接拒绝入队 |
| **不碰私密来源** | 私密频道/群组只能构造 `t.me/c/<id>/<msg>` 链接，这种链接依赖下载账号已加入该频道——等于绕过频道自身的可见性。本工具不做这条路径 |
| **拒绝时说明原因** | 不是静默丢弃，会回复用户"来源是私密频道，本工具只处理公开频道的转发" |

这里有个判断值得说明：**具备能力不等于应当默认使用**。tdl 确实能从受保护聊天下载，
但本工具的定位是「转发公开内容后归档」，因此不把这部分能力暴露出来。
如果你确实有权保存某个私密频道的内容，请通过官方客户端自行导出，那不是这个工具该做的事。

首次启动时也会在日志里打印一次使用须知。

## 它解决什么问题

Telegram 官方的"转发到收藏夹"没法按作者分类，频道内容也会过期。手动下载又太麻烦。

这个工具让你在 Telegram 里给 bot 发一个标签、然后转发消息，剩下的自动完成：

- 从 `forward_origin` 解析出原始频道与消息 ID，构造可下载的链接
- 排队、下载、按 `作者_描述.ext` 规则命名
- 同名同大小自动去重，重名自动加序号，**永不覆盖**
- 面板里能看到进度、重试失败任务、维护作者库

## 五个值得一读的实现细节

代码不大（约 3.6k 行 Go），但下面三处踩过坑，写下来省得别人再踩。

### 1. 无锁的原子任务队列

多个 worker 抢同一个队列，朴素的 `SELECT ... WHERE status='pending'` + `UPDATE` 会重复消费同一条。
常见解法是开事务加锁，或者用 `SELECT ... FOR UPDATE SKIP LOCKED`（SQLite 不支持）。

这里用一条 SQL 完成「挑出来 + 标记为处理中」：

```sql
UPDATE queue SET status='processing', updated_at=?
WHERE id IN (
    SELECT id FROM queue
    WHERE status='pending' AND COALESCE(retry_after,0) <= ?
    ORDER BY id LIMIT ?
)
RETURNING id, chat_id, tag, media_type, url, retry_count, ...
```

`RETURNING` 让这条语句同时完成写入和读取，SQLite 的行级锁保证并发下不会两个 worker 拿到同一行。
`retry_after` 是退避时间戳——失败任务在冷却期内不会被重新捞出来，因此不需要让 worker `sleep` 占着并发位。

### 2. tdl 的进度是 `\r` 刷新型的

`tdl` 的进度条用回车原地刷新，而 `bufio.Scanner` 默认按 `\n` 切分。直接用 `Scan()` 只能拿到最后一行，
中间帧全部丢失。

解法是自定义 `SplitFunc`，同时按 `\r` 和 `\n` 切：

```go
func splitOnCRLF(data []byte, atEOF bool) (int, []byte, error) {
    if i := bytes.IndexAny(data, "\r\n"); i >= 0 {
        return i + 1, data[:i], nil
    }
    if atEOF && len(data) > 0 {
        return len(data), data, nil
    }
    return 0, nil, nil
}
```

另外两件事：

- **节流**：至少间隔 3 秒或进度跳变 ≥10% 才更新一次消息，否则会撞 Telegram 的编辑频率限制
- **速度要自己算**：tdl 的输出只有百分比，没有速率。于是每 3 秒采样一次下载目录的大小增量，
  自己算出 MB/s。为了并发时各算各的，每个任务先下到 `downloads/.staging/t<id>/`，完成后再 `rename` 归位

### 3. 测速窗口短于 5 秒，数据就是假的

用脚本遍历代理节点测带宽、自动切到最快的一个，听起来很直接。第一版用了 3 秒窗口，结果：

| 节点 | 3 秒窗口 | 预热 1s + 计时 4s |
|---|---|---|
| Japan 01 | 0.16 MB/s | **10.35 MB/s** |
| Japan 02 | 0.88 MB/s | **11.09 MB/s** |
| Hong Kong 06 | 0.70 MB/s | **8.05 MB/s** |

**相差 60 倍。** 因为短窗口里 TCP 慢启动、连接建立、DNS 解析占比过大，宽带节点全被误判成窄带。

修正后的测速流程：先读 1 秒丢弃，再计时 4 秒。选出来的节点才靠谱。

还有一条同样重要：**测速目标和业务目标不是一回事**。用 Cloudflare 测出来的最快节点，
可能连 Telegram 根本不通。

### 4. 「带宽优先」会把自己锁死在死节点上

最初的实现是：在测速最快的**三个**节点里，挑第一个能连上 `api.telegram.org` 的。
如果三个都连不上呢？当时的处理是——**保持当前节点不动**。

这行代码是所有后续故障的根源。因为当前节点恰恰可能就是那个死节点：

> 切到不通的节点 → 测速发现最快的三个也都不通 → 保持当前节点不动
> → 而当前节点仍是那个不通的 → 永远出不来。

表现就是：进程活着、心跳在写、面板能打开，但**一条消息也处理不了**，
可以连续空转好几小时。

正确做法是**可达性优先于带宽**：

1. 逐个节点测速，按带宽排序
2. 遍历排序结果，跳过连不上 Telegram 的（而不是只看前三个）
3. 如果一个都不通，改用 mihomo 的 per-node delay 接口再扫一遍全量节点
   （这个接口能直接指定单个节点测延迟，不需要切换策略组，目标地址就是 Telegram 本身）
4. 只要有一个能通就切过去

配套的还有一条**应急救援**：轮询连续失败 5 次就立即换一次节点（5 分钟冷却），
不等周期测速跑完。实测从故障到自愈 **39 秒**。

### 5. HTTP/2 的半开连接会让程序彻底静默

这是最难查的一个坑，症状极具迷惑性。

代理切到不通的节点后，**已建立的 HTTP/2 连接会变成半开状态**：
请求挂在 `net/http.(*http2ClientConn).roundTrip` 的 select 上，**永不返回**。
更糟的是，`context` 超时对它无效——请求已经交给 h2 的连接池排队了，
取消 context 只是标记、不会唤醒那个 select。

于是：

- 没有一条失败日志（因为请求根本没返回，不算失败）
- 失败计数永远是 0，所以任何基于「失败次数」的自愈逻辑都不会触发
- 健康指标一路上涨，但没有任何机制去读它

抓栈是定位它的唯一办法：

```
net/http.(*http2ClientConn).roundTrip(...)   ← 卡在这里
main.(*TGClient).GetUpdates(...)
main.runPoller(...)
```

两处修复：

**其一，禁用 HTTP/2**（`ForceAttemptHTTP2: false` + 清空 `TLSNextProto`）。
HTTP/1.1 的连接在 Read 时受 context 控制，半开连接会被超时打断，
错误也能正常冒泡到轮询失败路径。

**其二，加一层独立看门狗**。就算请求挂在哪，看门狗也能从外部强行打断：

```go
hardLimit := time.Duration(timeoutSec)*time.Second + 45*time.Second
select {
case r := <-ch:
    return r.ups, r.err
case <-timer.C:
    cancel()
    c.forceRebuild()   // 半开连接只存在于被丢弃的 Transport 里
    return nil, fmt.Errorf("长轮询超时（%s），已强制中断并重建连接池", hardLimit)
}
```

### 附带：怎么判断「假活」

Go 服务常见的一种故障是：进程活着、心跳文件在写、systemd 显示 `active`，但一条消息都收不到
（HTTP 连接池被半死连接占满）。

所以健康检查不看进程状态，看**最近一次成功轮询的时间**：

```go
if time.Now().Unix()-lastPollOK.Load() > 300 {
    w.WriteHeader(http.StatusServiceUnavailable)  // 供外部看护重启
}
```

同时客户端做了熔断：连续失败 5 次就整体重建 `http.Transport`——卡死的半开连接无法单独回收，只能整体丢弃。

> 顺带一提：`lastPollOK == 0`（从启动至今一次都没成功轮询过）要单独处理。
> 否则 `now - 0` 会算出几十亿，`poll_age` 显示成 `1792462292` 这种看不懂的数字。

## 快速开始

### 依赖

- Go 1.23+（编译时需要，运行时不需要）
- [`tdl`](https://github.com/iyear/tdl) —— 负责实际的 Telegram 下载，需要先 `tdl login` 登录一次
- 一个能访问 Telegram 的代理

### 编译

```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o tg-archiver .
# 交叉编译到 Linux NAS
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o tg-archiver .
```

单文件约 9.7 MB，静态链接，拷到目标机直接跑。

### 运行

```bash
cp .env.example .env
# 填 BOT_TOKEN / OWNER_ID / PANEL_TOKEN
./tg-archiver
```

systemd 单元见 `deploy/tg-archiver.service`。

### 使用

在 Telegram 里：

| 操作 | 说明 |
|---|---|
| `#标签名` | 设置当前标签（同时也是作者名，会自动加入作者库） |
| 转发消息给 bot | 入队并下载 |
| `/status` `/queue` | 查看统计与最近任务 |
| `/retry <id>` | 重试失败任务 |
| `/panel` | 获取面板地址 |

## 文件命名

标签就是作者。归档路径 `downloads/<标签>/`，文件名 `作者_描述.ext`：

| 原始文件名 | 标签 | 归档为 |
|---|---|---|
| `城市夜景延时摄影 (10).mp4` | Alice | `Alice_城市夜景延时摄影 (10).mp4` |
| `11.mp4` | Bob | `Bob_11.mp4` |
| `trim_head_5e4df41b.mp4` | StudioX | `StudioX.mp4` |
| `IMG_9243.MP4` | Cam01 | `Cam01_9243.mp4` |
| `DeltaStudio-00.00.00.000-seg1.mp4` | Bob | `DeltaStudio_seg1.mp4` |

判定"无意义"：纯数字 / 长哈希 / `trim_head_`、`IMG_`、`VID_` 等工具前缀 / 只剩分片号。
这类会用标签顶替，但**保留原序号**（`11.mp4` → `Bob_11.mp4`），否则同一标签下多个文件会撞成同名。

作者库支持别名归一（`StudioX` / `studiox` / `sx` 视为同一人），文件名里出现作者名时会优先采用
——标签打错了也能靠文件名纠正。

## 配置

见 `.env.example`，全部通过环境变量。关键项：

| 变量 | 默认 | 说明 |
|---|---|---|
| `BOT_TOKEN` | 必填 | |
| `OWNER_ID` | 0 | 0 = 不限制；非 0 = 仅该用户可用 |
| `PANEL_TOKEN` | **必填** | 面板可改队列，不允许无认证 |
| `MAX_WORKERS` | 2 | 并发任务数 |
| `TDL_THREADS` | 16 | 单文件分片数 |
| `NODE_TEST_ENABLED` | false | 是否自动测速选节点 |

## HTTP API

面板与接口共用 `PANEL_TOKEN` 认证（`Authorization: Bearer <token>`）。

```
GET    /healthz                      健康检查（轮询停滞返回 503）
GET    /api/stats                    统计、连接状态、轮询距上次成功时间
GET    /api/queue?status=&limit=     任务列表
POST   /api/queue/{id}/retry         重新入队
POST   /api/queue/{id}/cancel        取消
POST   /api/queue/{id}/tag           改标签
POST   /api/cleanup                  触发缓存清理
GET    /api/authors                  作者库
POST   /api/authors/bulk             批量导入：{"text":"名字,别名1,别名2\n..."}
GET    /api/authors/preview?name=&tag=  预览最终文件名（不落盘）
GET    /api/events?since=&unacked=   增量事件（供外部 agent 消费）
POST   /api/alert                    外部写入事件，统一告警出口
```

事件表是给自动化留的口子：任务重试耗尽、轮询连续失败、节点切换都会写一条，
配合 `ALERT_WEBHOOK` 可以推给任意告警通道。

## 已知限制

- **下载速度受 Telegram 账号限制**，实测普通账号在 ~2 MB/s 附近封顶。换节点、加分片、加并发都突破不了——这是账号级/DC 级限制，不是本地瓶颈
- `tdl` 的 session 是独占的，多个 worker 必须各用一个 storage 副本（代码里用 `.tdl_workers/w0..wN` 隔离）
- 只处理 `message` 类 update，不支持频道自动订阅
- SQLite 的 `CURRENT_TIMESTAMP` 存的是 UTC，与日志时间差 8 小时，排查时注意

## 许可证

MIT。依赖仅 `modernc.org/sqlite`（BSD-3-Clause），无 CGO、无 GPL 污染。
