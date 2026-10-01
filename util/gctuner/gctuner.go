// Package gctuner 根据内存水位动态调整 GOGC。
// 使用 runtime.AddCleanup 实现每轮 GC 后的调参钩子（需要 Go 1.24+）。
//
// 用法：
//
//	gctuner.Tuning(uint64(4 << 30 * 0.7)) // 水位设为 4GB 的 70%
package gctuner

import (
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"

	"github.com/spf13/cast"
)

var (
	maxGCPercent uint32 = 500
	minGCPercent uint32 = 50
)

var defaultGCPercent uint32 = 100

func init() {
	gogcEnv := os.Getenv("GOGC")
	gogc, err := cast.ToIntE(gogcEnv)
	if err != nil {
		return
	}
	defaultGCPercent = uint32(gogc)
}

// Tuning 设置 GC 调参的高水位（字节数）。
// 开启后 GOGC 环境变量失效，GCPercent 由 tuner 动态接管。
// threshold == 0 表示停止调参。
func Tuning(threshold uint64) {
	if threshold <= 0 && globalTuner != nil {
		globalTuner.stop()
		globalTuner = nil
		return
	}
	if globalTuner == nil {
		globalTuner = newTuner(threshold)
		return
	}
	globalTuner.setThreshold(threshold)
}

// GetGCPercent 返回当前的 GCPercent。
func GetGCPercent() uint32 {
	if globalTuner == nil {
		return defaultGCPercent
	}
	return globalTuner.getGCPercent()
}

// GetMaxGCPercent 返回 GCPercent 上限。
func GetMaxGCPercent() uint32 { return atomic.LoadUint32(&maxGCPercent) }

// GetMinGCPercent 返回 GCPercent 下限。
func GetMinGCPercent() uint32 { return atomic.LoadUint32(&minGCPercent) }

// SetMaxGCPercent 设置 GCPercent 上限，返回旧值。
func SetMaxGCPercent(n uint32) uint32 { return atomic.SwapUint32(&maxGCPercent, n) }

// SetMinGCPercent 设置 GCPercent 下限，返回旧值。
func SetMinGCPercent(n uint32) uint32 { return atomic.SwapUint32(&minGCPercent, n) }

// 一个进程只允许一个 tuner
var globalTuner *tuner

/*
	Heap
	_______________  => limit: host/cgroup memory hard limit

|               |
|---------------| => threshold: increase GCPercent when gc_trigger < threshold
|               |
|---------------| => gc_trigger: heap_live + heap_live * GCPercent / 100
|               |
|---------------|
|   heap_live   |
|_______________|

Go runtime 只在堆到达 gc_trigger 时触发 GC，gc_trigger 由 GCPercent 和存活堆决定。
因此每轮 GC 后动态调整 GCPercent，就能控制 GC 的松紧。
*/
type tuner struct {
	stopped   atomic.Int32
	gcPercent atomic.Uint32
	threshold atomic.Uint64 // 高水位，字节数
}

func newTuner(threshold uint64) *tuner {
	t := &tuner{}
	t.gcPercent.Store(defaultGCPercent)
	t.threshold.Store(threshold)
	t.arm() // 启动 GC 接力钩子
	return t
}

func (t *tuner) stop() {
	t.stopped.Store(1) // 不再 arm 新祭品，接力自然终止
}

func (t *tuner) setThreshold(threshold uint64) { t.threshold.Store(threshold) }

func (t *tuner) setGCPercent(percent uint32) {
	t.gcPercent.Store(percent)
	debug.SetGCPercent(int(percent))
}

func (t *tuner) getGCPercent() uint32 { return t.gcPercent.Load() }

// arm 造一个一次性"祭品"对象：它离开作用域后无人引用，
// 下一轮 GC 回收它时触发 cleanup
func (t *tuner) arm() {
	if t.stopped.Load() > 0 {
		return
	}
	ref := new(byte)
	runtime.AddCleanup(ref, func(t *tuner) {
		if t.stopped.Load() > 0 {
			return
		}
		t.tuning()
		t.arm() // 造下一代祭品 下一次gc的时候继续
	}, t)
}

// tuning 在每轮 GC 后调用：读当前堆占用，动态计算并下发新的 GCPercent。
// runtime 保证 cleanup 串行执行，这里无需加锁。
func (t *tuner) tuning() {
	threshold := t.threshold.Load()
	if threshold <= 0 {
		return
	}
	t.setGCPercent(calcGCPercent(readMemoryInuse(), threshold))
}

// threshold = inuse + inuse * (gcPercent / 100)
// => gcPercent = (threshold - inuse) / inuse * 100
//
// threshold < inuse*2 时 gcPercent < 100：内存逼近水位，勤 GC 防 OOM
// threshold > inuse*2 时 gcPercent > 100：内存宽裕，懒 GC 省 CPU
func calcGCPercent(inuse, threshold uint64) uint32 {
	if inuse == 0 || threshold == 0 {
		return defaultGCPercent
	}
	// 在使用的内存已经 > 阈值了 需要最激进的方案 gc
	if threshold <= inuse {
		return minGCPercent
	}
	gcPercent := uint32(math.Floor(float64(threshold-inuse) / float64(inuse) * 100))
	if gcPercent < minGCPercent {
		return minGCPercent
	} else if gcPercent > maxGCPercent {
		return maxGCPercent
	}
	return gcPercent
}

var memStats runtime.MemStats

func readMemoryInuse() uint64 {
	runtime.ReadMemStats(&memStats)
	return memStats.HeapInuse
}
