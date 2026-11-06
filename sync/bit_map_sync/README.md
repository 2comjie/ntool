# bit_map_sync

`BitmapSync[K]` 在 Redis 中保存原始 Bitmap 字节，用 `GETBIT/SETBIT` 读写，定期把整张 Bitmap 批量同步到 DB。不同 Bitmap key 可以由多个实例并行同步。

```go
s := bit_map_sync.New[int64](rc, bitmapDb, bit_map_sync.Config{
    Prefix:    "checkins",
    Interval:  time.Second,
    BatchSize: 100,
})

err := s.SetBit(ctx, userId, dayOffset, true)
checked, err := s.GetBit(ctx, userId, dayOffset)
err = s.SetBit(ctx, userId, dayOffset, false) // 清除单个 bit
err = s.Delete(ctx, userId)                // 删除整张 Bitmap
err = s.Flush(ctx)                        // 同步最多 BatchSize 张 Bitmap
```

在应用管理的 goroutine 中执行阻塞的 `s.Run(ctx)` 启动定期同步，取消 context 后返回；构造函数不启动 goroutine。实例共享同一个且由该业务独占的 Prefix。

DB 接口：

```go
type DbHandler[K comparable] interface {
    Get(ctx context.Context, key K) ([]byte, error)
    WriteBatch(ctx context.Context, keys []K, values [][]byte) error
    DeleteBatch(ctx context.Context, keys []K) error
}
```

DB 以 BLOB/二进制列保存完整字节数组，不用 JSON 编码 Bitmap。字节序与 Redis 相同：offset 0 是第一个字节的最高位，例如 `[]byte{0x80}` 表示 bit 0 为 1。keys 与 values 按位置对应，写入是幂等的完整覆盖，删除不存在的记录也应成功，handler 必须响应 context 取消。

`Get` 返回 `nil, nil` 表示不存在；非 nil 的空切片或全零字节数组表示存在的空/全零 Bitmap。读取这些 Bitmap 的 bit 均返回 false。不存在的 Bitmap 使用默认 5 秒的短期空值缓存；存在的数据使用默认 5 分钟的 CacheTTL，附加最多 10% 随机时间。

读写与同步流程：

- 缓存未命中时，按 key 合并跨实例回源，默认超时 5 秒。SetBit 也必须先加载完整 DB 数据，保留其他已经置位的 bit。
- SetBit 通过 Lua 原子修改 bit、移除 TTL、记录脏任务，并撤销旧回源占位。空 String 同样表示缓存已加载，因此 bit 为 0 不会被误判为未命中。
- Delete 无需回源，写入空 Bitmap 和独立删除标记；成功落库前两者都不过期。删除后再 SetBit 会从空 Bitmap 开始重建。
- Flush 通过 Lua 原子领取任务，按 key 隔离有效领取。处理中 key 的修改保留在 Redis，当前任务完成后比较内容及删除标记，发生变化则重新排队。
- 默认租约 30 秒，每隔 LeaseTTL/3 续租；失败释放任务供重试，实例崩溃后由其他实例在租约到期时接管。
- 快照 Lua 一次读取原始字节和删除标记，DB 全部成功后才确认。确认 Lua 校验 token、租约及快照，清除任务并设置 TTL；整张删除同步成功后，删除标记被清理，留下短期空值缓存。
- Run 每轮失败直接记录错误，后续 tick 继续重试；不保存后台错误状态。

存储与运行约定：

- offset 范围是 `[0, 2^32)`。SetBit 可能扩展 Bitmap，业务应按自己的数据范围选择 offset。
- 默认每实例每秒最多领取 100 张 Bitmap。BatchSize 限制条目数；回源、快照、落库及确认均涉及完整字节数组，数据量和内存占用随批次中 Bitmap 的总大小增长。本模块按整张同步，适合单张 Bitmap 较小的业务。
- Redis 保存未落库数据，使用 noeviction，并按允许的数据丢失窗口配置持久化和复制。脏 Bitmap 缺失会直接报错并保留任务，不回源覆盖成旧 DB 值。
- DB 同步为至少一次语义，部分操作成功或 DB 已提交但 Redis 确认失败时，整批可能重放。
- token 隔离 Redis 任务所有权。租约失效会取消本轮 context，但不能撤回已经执行的 DB 请求；严格阻止旧持有者在接管者写入后提交旧数据，需要 DB 端持久化单调版本/fencing，删除也保留版本。
- K 的 JSON 表示必须唯一且能够往返序列化。一个 Prefix 的 Redis key 使用相同 hash tag，保证 Lua 多 key 原子操作；更大规模可按业务拆分 Prefix。
- 一次 Flush 只同步一批，没有可领取任务时也返回 nil，不代表整个命名空间已经排空。停机需要排空时先停止业务写入，再确认共享待处理和处理中集合都为空。

Lua 全部放在独立 `.lua` 文件中，通过 `go:embed` 加载；领取、续租、释放脚本沿用 HashSync 的队列协议。
