# string_sync

业务通过 `StringSync[K, T]` 读写 Redis String；模块在缓存未命中时回源 DB，并定期批量落库。多实例使用同一个 `Prefix` 共享数据、脏集合、回源占位和同步锁。

```go
s := string_sync.New[int64, User](rc, userDb, string_sync.Config{
    Prefix:    "users",
    Interval:  time.Second,
    BatchSize: 1000,
    CacheTTL:  5 * time.Minute,
    MissTTL:   5 * time.Second,
})

// Run 阻塞执行，通常由应用的 goroutine/errgroup 管理生命周期。
go func() {
    if err := s.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Print(err)
    }
}()

err := s.Write(ctx, userId, &User{Name: "Alice"}) // 仅写 Redis
user, err := s.Get(ctx, userId)                  // 未命中时内部回源
err = s.Delete(ctx, userId)                     // 写入待同步的删除标记
err = s.Flush(ctx)                             // 手动同步最多 BatchSize 条
```

`DbHandler.Get` 对不存在的记录返回 `nil, nil`。`GetBatch` 返回值与 keys 按位置对应。DB 写入使用幂等 upsert，删除不存在的记录也应成功。`StringSync` 当前调用 handler 的 `Get`、`WriteBatch` 和 `DeleteBatch`，其余方法保留接口的单条/批量形状。`StringSync.GetBatch` 按输入顺序逐个读取，每个缺失 key 独立参与回源合并。

读写行为：

- 同一个缺失 key 通过 Redis 占位合并跨实例回源；回源默认超时 5 秒。超时占位失效后，新请求可继续回源，旧请求的回填会被拒绝。handler 必须响应 context 取消，才能及时停止超时查询。
- DB 中不存在的 key 缓存短期空值，减少重复穿透。不同的随机 key 仍可能分别回源；需要完整存在性索引时应由业务提供。
- 已落库的数据使用 TTL，并附加最多 10% 的随机时间，分散集中失效。
- 写入/删除与脏标记通过 Lua 原子提交，未落库的数据和删除标记没有 TTL。
- 写入会撤销正在进行的回源占位，防止旧 DB 查询覆盖新值。
- `Flush` 复用 `ntool/lock/redis` 的自动续租锁；版本未变化才清除脏标记并设置 TTL。失败保留脏记录，`Run` 当轮记录错误，下轮继续。
- 每次 tick 最多处理 `BatchSize` 条。默认每秒一批、每批 1000 条；实际同步能力还取决于 DB 批次耗时。多实例共享同一同步锁，不叠加批次吞吐。

存储与一致性约定：

- 所有读写经过模块；DB 被外部修改时，缓存到 TTL 到期前可能保持旧值。
- Redis 承担未落库数据的存储职责：使用 `noeviction`，并按允许的数据丢失窗口配置持久化和复制。Redis 丢失未落库数据时，模块不能从 DB 恢复这些修改。检测到脏数据缺失时直接报错，保留脏标记。
- DB 同步为至少一次语义。DB 已提交而 Redis 确认失败时，同一批数据可能再次写入。
- Redis 锁只在租约有效期间保证同步互斥。租约丢失会取消本轮 context；如果 DB 请求忽略取消、在新持有者完成后仍提交旧写入，当前接口不能阻止旧值覆盖。要求覆盖该故障场景时，DB 接口和存储必须加入单调版本/fencing 校验，删除也必须保留版本。
- K、T 必须支持 JSON 往返序列化，K 的 JSON 表示必须唯一。Redis 中保存的是含 key、value、版本标记的 JSON String。
- 同一 Prefix 的 key 使用相同 Redis Cluster hash tag，保证 Lua 原子操作；一个命名空间落在一个 slot。大数据量可按业务分片创建多个 Prefix。
- 停机时先停止业务写入，再停止并等待 `Run` 返回，最后使用有效 context 重复调用 `Flush`，直到业务确认待同步数据已排空。一次 `Flush` 只处理一批，锁被其他实例持有时直接返回 nil，不表示整个命名空间已同步完成。
