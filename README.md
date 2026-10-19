# tg-archiver

把 Telegram 频道/群组里转发过来的媒体，按标签归档到本地 NAS。

单二进制、零运行时依赖、内置 Web 面板。用 Go 写调度层，用 [`tdl`](https://github.com/iyear/tdl) 做实际下载。

```
Telegram Bot API ──长轮询──> poller ──> SQLite 队列 ──> worker pool ──> tdl ──> 本地目录
                                            ↑                              │
                                       Web 面板                    进度回写 / 事件上报
```

## 它解决什么问题

Telegram 官方的"转发到收藏夹"没法按作者分类，频道内容也会过期。手动下载又太麻烦。

这个工具让你在 Telegram 里给 bot 发一个标签、然后转发消息，剩下的自动完成：

- 从 `forward_origin` 解析出原始频道与消息 ID，构造可下载的链接
- 排队、下载、按 `作者_描述.ext` 规则命名
- 同名同大小自动去重，重名自动加序号，**永不覆盖**
- 面板里能看到进度、重试失败任务、维护作者库

## 三个值得一读的实现细节

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
可能连 Telegram 根本不通。所以最终在最快的三个里逐个验证 `api.telegram.org` 可达，才真正切换。

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
