# fsm

`github.com/2comjie/ntool/fsm` 是在 Actor 主循环等串行环境下使用的泛型状态机。

```go
machine := fsm.New("waiting", []fsm.Transition[string, string]{
    {
        From:   "waiting",
        Event:  "start",
        To:     "bidding",
        Before: room.checkReady,
        After:  room.dealCards,
    },
})

if err := machine.Fire("start"); err != nil {
    return err
}
phase := machine.State()
```

`Fire` 按当前状态和事件查表。查不到就返回错误。`Before` 可返回业务错误来拒绝转换。`After` 在新状态生效后同步执行。两种回调都是可选的，同一状态与事件的转换规则必须唯一。

阶段超时由业务的定时器投递事件。RoomActor 进入限时阶段时用 `Self.After` 注册任务，成功离开阶段时取消。定时回调在同一个 Actor 主循环里触发 FSM 的超时事件。
