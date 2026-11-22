# fsm2

`fsm2` 用 `SimpleState` 注册状态，按需填上生命周期和事件回调。调用方串行调用 `Update(dt)` 和 `TriggerEvent(event)`。

```go
machine := fsm2.New("idle")
machine.Register("idle", fsm2.SimpleState[string]{})
machine.Register("bidding", fsm2.SimpleState[string]{
    PreEnter: func(_ string, _ bool) time.Duration {
        return 15 * time.Second
    },
    OnEvent: func(event any) {
        if event == "landlord-selected" {
            machine.Switch("playing")
        }
    },
    NextOnTimeout: func() string {
        return "playing"
    },
})
machine.Register("playing", fsm2.SimpleState[string]{
    PostEnter: func(_ string, timedOut bool) {
        if timedOut {
            // 先按游戏规则选定地主。
        }
        // 开始出牌。
    },
})

machine.Switch("bidding")
machine.Update(15 * time.Second) // 超时切到 playing。
```

切换依次执行全局 `BeforeSwitch`、旧状态的 `PreExit`、新状态的 `PreEnter`，然后更新状态和时长，再执行 `PostExit`、`PostEnter` 和全局 `AfterSwitch`。生命周期回调请求的切换按调用顺序在当前切换结束后执行。显式切到当前状态也会重新执行这套流程。

初始状态需要注册，首次 `Switch` 进入阶段时才执行进入回调。没有 `PreEnter` 或返回负数时不计时，返回零则在下一次 `Update` 超时。限时状态需要提供 `NextOnTimeout`。单次 `Update` 最多触发一次自动超时，超出旧状态剩余时长的 `dt` 不计入新阶段。
