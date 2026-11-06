# dirty_sync

通用脏 key 同步组件，只管理标脏、领取、续租、确认和重排。业务数据的读写、删除含义、缓存策略和落库逻辑全部由上层决定。

```go
s := dirty_sync.New[string](rc, dirty_sync.Config{
    Prefix:    "im:{shard1}:sync",
    Interval:  time.Second,
    BatchSize: 1000,
    LeaseTTL:  30 * time.Second,
}, func(ctx context.Context, keys []string) error {
    return saveLatestValuesToDb(ctx, keys)
})

err := s.MarkDirty(ctx, "conversation-1", "conversation-2")
err = s.Flush(ctx) // 最多领取一批
```

在应用管理的 goroutine 中调用阻塞的 `s.Run(ctx)` 启动定期同步。取消 context 后 Run 返回；处理失败会当轮记录错误并在后续 tick 重试。构造函数不启动 goroutine。

与 Redis 业务写入一起标脏：

```go
_, err := rc.TxPipelined(ctx, func(tx redis.Pipeliner) error {
    tx.HSet(ctx, "im:{shard1}:messages", messageId, body)
    return s.MarkDirtyTx(ctx, tx, messageId)
})
```

`MarkDirtyTx` 只将完整 EVAL 命令加入 Pipeline，序列化失败立即返回，执行期错误由上层 TxPipelined/Exec 返回。普通 Pipeline 不保证业务写入与标脏之间的原子性；Redis 事务不会回滚执行期错误，调用者需要处理 Exec 的错误。

同一队列的实例使用相同的 Prefix、K 类型和 Handler 语义。Prefix 原样用于 `<Prefix>:pending`、`:working`、`:owners`、`:redirty`，不自动添加 hash tag。Cluster 中 Prefix 必须包含合适的 hash tag，使四个元数据 key 以及同一事务中的业务 key 位于同一 slot。K 使用 JSON 编码作为任务身份，必须能往返序列化且表示唯一；复合 key 可以用带导出字段的 struct。

任务规则：

- 待处理 key 重复标脏会合并，不改变首次排队时间。
- Lua 原子领取最多 BatchSize 个 key，记录随机 token 和 Redis 时钟计算的租约。多个实例的有效领取不重叠。
- 处理中再次标脏会记录到 redirty。Handler 成功后只确认当前任务，再将 redirty key 重新排队；组件不读取或比较业务数据。
- 默认每隔 LeaseTTL/3 续租。续租失败或失去所有权时取消 Handler context。Handler 错误或调用取消时，尝试在独立的有效 context 中释放本轮任务。
- 实例崩溃后，后续 Flush 分批回收租约到期的任务。旧 token 不能续租、确认或释放新持有者的任务。
- Handler 返回错误会整批重排，部分外部写入可能已经成功，所以 Handler 必须幂等并响应 context 取消。DB 已提交但 Redis 确认失败时同样可能重放。

运行边界：

- 默认 Interval 为 1 秒、BatchSize 为 1000、LeaseTTL 为 30 秒。缺少 Redis、Prefix、Handler 或配置非法时直接 panic。
- Flush 返回 nil 只表示本轮没有错误；没有可领取任务也返回 nil，不代表所有实例的处理中任务已经结束。
- 业务数据的修改与标脏由上层保证一致提交。直接分开调用写入和 MarkDirty，会存在写入后进程退出、尚未标脏的窗口。
- 业务数据至少保留到 Handler 能读取并完成同步；组件无法恢复被提前过期或裁剪的数据。Redis 中的任务元数据不能被淘汰，持久化和复制按允许的丢失窗口配置。
- Redis token 隔离队列所有权，无法撤回 Handler 已提交的外部操作。严格防止失去租约的旧 Handler 晚提交，需要上层存储配合单调版本/fencing。

所有 Lua 均保存在独立 `.lua` 文件中，通过 `go:embed` 加载。
