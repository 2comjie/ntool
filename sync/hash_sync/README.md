# hash_sync

一个 Redis Hash 保存一个业务集合，一个 field 保存一条独立的 T。例如 `key=玩家Id`、`field=物品Id`、`value=物品数据`。所有业务读写经过 `HashSync`，内部处理未命中回源和定期落库。

```go
s := hash_sync.New[int64, int64, Item](rc, itemDb, hash_sync.Config{
    Prefix:    "items",
    Interval:  time.Second,
    BatchSize: 1000,
})

err := s.Write(ctx, playerId, itemId, &Item{Count: 10})
item, err := s.Get(ctx, playerId, itemId)
err = s.Delete(ctx, playerId, itemId)
err = s.Flush(ctx)
```

在应用管理的 goroutine 中调用阻塞的 `s.Run(ctx)` 启动定期同步；取消 context 后返回。构造函数不启动后台任务。所有实例使用相同的 Prefix；Prefix 必须由这份业务数据独占。

`DbHandler` 沿用以下接口：

```go
type DbHandler[K comparable, F comparable, T any] interface {
    Get(ctx context.Context, key K, field F) (*T, error)
    WriteBatch(ctx context.Context, key K, fields []F, values []*T) error
    DeleteBatch(ctx context.Context, key K, fields []F) error
}
```

`Get` 不存在时返回 `nil, nil`。写入与删除必须幂等，并响应 context 取消。**批量写入只更新指定 fields，不能替换整条集合**，因为同一 key 的不同 fields 可以由不同实例并行同步。fields 与 values 按位置对应。

Redis 中的数据：

| 结构 | 用途 |
| --- | --- |
| 每个 key 的数据 Hash | field 对应含 key、field、value 的 JSON；value 为 null 表示待同步删除 |
| 每个 key 的脏字段 Set | 标记所有未落库 fields，包括正在同步的 fields；非空时整个 Hash 不过期 |
| 命名空间的待处理 ZSet | 待领取的 `(key, field)`，按首次排队时间排序 |
| 命名空间的处理中 ZSet | 已领取的 `(key, field)`，score 为 Redis 时钟计算的租约截止时间 |
| 命名空间的领取者 Hash | task 对应随机领取 token，隔离旧领取者的续租、确认、释放 |
| 每个 field 的短期 String | 回源占位、空值缓存；分别使用独立 TTL |

写入 Lua 原子更新数据、标记脏字段、撤销旧回源及空值缓存，并移除整个 Hash 的 TTL。处理中 field 的新写入保留在数据 Hash 和脏字段 Set 中，当前任务结束时按内容比较，发生变化则重新排队。

`Flush` 通过独立 Lua 原子回收过期任务并领取最多 BatchSize 个 fields，无全局 Flush 锁。各实例的有效领取互不重叠；领取的 fields 按业务 key 分组写入 DB。同一个 field 在处理期间不会被再次领取，其他 fields 可以继续并行。

租约默认 30 秒，每隔 LeaseTTL/3 续租。DB 全部成功后，Lua 检查领取者和租约，比较本轮内容，清除已同步的脏字段；删除标记同步成功后才真正 HDEL，并写入短期空值缓存。DB 失败则释放本轮任务，后续调用重试；实例崩溃时由其他实例在租约到期后接管。Run 每轮失败直接记录错误，不保存后台错误状态。

读取未命中按 `(key, field)` 合并跨实例回源，默认超时 5 秒。空值缓存默认 5 秒，只减少相同缺失 field 的重复查询；不同随机 field 仍会分别回源。旧回源 token 被写入或超时接管撤销后，不允许回填。

完整 Hash 仅在没有任何脏字段时设置 CacheTTL，默认 5 分钟并随机增加最多 10%。这是整 Hash 的 TTL，不是每个 field 独立的刷新期限；其他字段落库也可能延长它。Hash 中可以只有部分已加载 fields，因此没有提供代表完整业务集合的 GetAll。

边界约定：

- 默认每秒每实例最多领取 1000 个 fields，实际吞吐还取决于 DB 耗时；一次 Flush 只处理一批，返回 nil 不代表整个命名空间已经排空。
- Redis 保存未落库数据，必须使用 noeviction，并按允许的数据丢失窗口配置持久化和复制。检测到脏字段数据丢失时直接报错，保留任务，不用旧 DB 值覆盖。
- DB 为至少一次同步语义。部分 DB 操作成功或 DB 已提交但 Redis 确认失败时，整批可以再次执行。
- token 只隔离 Redis 任务所有权。租约丢失后会取消同步 context，但无法撤回 DB 已经执行的请求；如果旧 DB 请求在接管者写入后仍然提交，当前 DbHandler 无法阻止旧值覆盖。严格覆盖该故障场景需要 DB 端持久化单调版本/fencing，删除也保留版本。
- K、F 的 JSON 表示必须唯一，K、F、T 必须能往返序列化。数据与内部元数据在同一个 Redis Cluster slot，保证 Lua 多 key 操作原子；更大规模可按业务拆分 Prefix。
- 停机取消 Run 只停止当前实例；需要排空时先停止业务写入，再确认共享待处理和处理中集合均为空。

所有 Lua 都存放在本目录的独立 `.lua` 文件中，通过 `go:embed` 加载。
