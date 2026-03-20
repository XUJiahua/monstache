# Plan: Recover 性能优化

## Context

当前 recover 流程完全串行：读文件 → 攒 batch → 查 MongoDB → Route 到 sink → Flush → 写进度 → 再读文件。flush 期间读文件完全停住。从生产日志看每 10000 行约 10 秒，6000 万条需要很长时间。

**线程安全约束**：`Sink.RouteData/RouteDelete` 和 `Flush()` 必须在同一个 goroutine 调用（keepers map 无锁保护）。`mongo.Client` 的 `Find` 是线程安全的。

## 改动概览

修改文件：`recover.go`、`monstache.go`（config 新增字段）

### 1. 可配置 batch size

`ConfigOptions` 新增 `RecoverBatchSize int` 字段（toml: `recover-batch-size`），默认 1000。

### 2. 读写 Pipeline

```
  reader goroutine                    flusher goroutine (单线程，保证 sink 安全)
  ┌─────────────┐                     ┌──────────────────┐
  │ 读文件/解压   │ ──batchCh──→       │ flushBatch       │
  │ JSON 解析    │  (buffered chan)    │ Route + Flush    │
  │ 去重/攒 batch │                    │ 更新进度          │
  └─────────────┘                     └──────────────────┘
```

- `batchCh` 为 buffered channel（容量 2），reader 攒满 batch 就发送，不等 flush 完成
- flusher 从 channel 消费，顺序处理，保证 RouteData/Flush 单线程调用
- 每个 batch 携带 `lineNum` 信息，flusher flush 成功后更新进度
- reader 结束后 close channel，flusher 处理完所有 batch 后返回

定义 batch 消息结构：
```go
type recoverBatch struct {
    entries map[dedupKey]dedupEntry
    lineNum int64
}
```

`recoverFile` 改为：启动 reader goroutine 往 batchCh 发 batch，主 goroutine 作为 flusher 消费 batchCh。

### 3. 并行查 MongoDB

`flushBatch` 内按 namespace 分组后，用 `errgroup` 并发查不同 namespace 的文档：

```go
g, ctx := errgroup.WithContext(ctx)
for ns, ops := range grouped {
    g.Go(func() error {
        foundDocs, err := fetcher.FetchByIDs(ctx, ns, oids)
        // 存结果到 per-ns 的 map
    })
}
g.Wait()
// 然后串行 RouteData/RouteDelete（单线程安全）
```

MongoDB 查询并行，Route 到 sink 仍然串行。

### 4. 进度追踪

不变：flusher 每处理完一个 batch 后写进度。由于 flusher 是单线程且按顺序消费 batch，进度仍然是线性单调递增的，断点续传逻辑不需要改。

## 具体改动

### `monstache.go`
- `ConfigOptions` 新增 `RecoverBatchSize int \`toml:"recover-batch-size"\``
- flag 注册 + config merge

### `recover.go`

1. 新增 `recoverBatch` struct
2. `doRecover`: 从 config 读 batchSize，传给 `recoverFile`
3. `recoverFile` 重构为 pipeline：
   - 启动 reader goroutine：读文件 → 过滤 → 去重 → 攒 batch → 发 `batchCh`
   - 主 goroutine 作为 flusher：消费 `batchCh` → `flushBatch` → 更新进度
   - reader 错误通过 `errCh` 传回
4. `flushBatch` 内 MongoDB 查询用 `errgroup` 并行，Route 仍串行
5. `recoverBatchSize` 常量改为从参数传入

### `recover_test.go`
- 现有测试自动适配（`doRecover` 签名不变）
- 新增 pipeline 并发测试

## Verification

1. `go build` 编译通过
2. `go test -run TestRecover -v -race` 全部通过且无 race
3. 对比优化前后处理相同文件的速度
