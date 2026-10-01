package gctuner

import (
	"runtime"
	"testing"
	"time"
)

func TestTuningRelay(t *testing.T) {
	// 制造一些存活内存，让 inuse 远离零
	hold := make([][]byte, 0, 64)
	for i := 0; i < 64; i++ {
		hold = append(hold, make([]byte, 1<<20)) // 64MB
	}

	Tuning(64 << 30) // 水位 64GB：inuse 远低于水位 → 应被顶到上限 500
	for i := 0; i < 10; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond) // cleanup 异步执行，等它
		if GetGCPercent() == maxGCPercent {
			break
		}
	}
	if got := GetGCPercent(); got != maxGCPercent {
		t.Fatalf("GCPercent = %d, want %d（内存宽裕应放宽到上限）", got, maxGCPercent)
	}

	// 水位调到贴近当前占用 → 应被压下来
	Tuning(96 << 20) // 96MB，仅略高于 64MB 存活
	for i := 0; i < 10; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
		if GetGCPercent() < 100 {
			break
		}
	}
	if got := GetGCPercent(); got >= 100 {
		t.Fatalf("GCPercent = %d, want < 100（逼近水位应收紧）", got)
	}

	// 停止调参
	Tuning(0)
	if globalTuner != nil {
		t.Fatal("Tuning(0) 后 globalTuner 应置 nil")
	}

	runtime.KeepAlive(hold)
}
