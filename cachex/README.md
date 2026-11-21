# cachex

`cachex` 是本地 LRU、Redis 和持久层组成的三级读缓存。本地淘汰使用 `hashicorp/golang-lru/v2/simplelru`。业务负责提交持久层事务，提交成功后调用 `Invalidate`。用同一个 `Prefix` 的服务实例会收到失效通知。

```go
type Profile struct {
    Name string
}

profiles := cachex.New(rc, loadProfile, cachex.Config{
    Prefix:   "prod:player-profile",
    LocalTTL: 5 * time.Second,
    RedisTTL: 5 * time.Minute,
})

// 由应用管理 Run 的生命周期，取消 ctx 后等待它退出。
go func() {
    if err := profiles.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Print(err)
    }
}()

profile, found, err := profiles.Get(ctx, playerId)
```

`loadProfile` 的签名为 `func(context.Context, int64) (Profile, bool, error)`。持久层查无记录时返回 `found=false, err=nil`。查询失败直接返回错误。所有实例使用相同的 Key/Value 类型和序列化约定。Key 的 JSON 表示必须唯一，解码后应与原 key 相等。Value 支持 JSON 往返，缓存返回的 map、slice、指针对象按只读快照使用。

改名时先提交持久层事务，成功后调用 `profiles.Invalidate(ctx, playerId)`。如果返回失效错误，事务可能已经提交，此时单独重试 `Invalidate`。新增和删除记录也用同一个接口。可能绕过应用直接写持久层的工具，同样需要触发失效。

- **读路径**先查本地，再查 Redis，最后调用 `Loader`。同进程同 key 的等待者可以独立取消，跨进程由 Redis 占位合并回源。Loader 要响应超时或取消。Redis 命令出错会返回错误，避免故障时把所有缓存 miss 转成持久层查询。
- **过期**默认本地容量 10000 条、TTL 5 秒、Redis TTL 5 分钟、不存在的记录缓存 2 秒、持久层加载超时 5 秒。L1 TTL 不长于 Redis 剩余 TTL。
- **失效**原子删除 Redis 值和回源占位，并在命名空间频道发布通知。占位 token 隔离过期查询。本地收到通知后同时清除值和当前回源，旧查询不能重新填回 L1。
- **订阅**在确认后开启 L1。检测到断开后清空 L1，期间读取走 Redis 和持久层，重新确认订阅时再启用。本地读取之后的其他实例失效异步到达，Pub/Sub 丢通知用 TTL 收敛。

持久层提交和 Redis 失效之间存在进程崩溃窗口。需要失效可靠补发的业务，可以在持久层事务里写 outbox，再由后台执行 `Invalidate`。`cachex` 与先写 Redis 再异步落库的 `string_sync` 分开使用。
