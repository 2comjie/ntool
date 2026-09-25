package mcache

import (
	"math/bits"
	"sync"
	"unsafe"

	"github.com/2comjie/ntool/lang/dirtymake"
)

const maxSize = 46

var caches [maxSize]sync.Pool

func init() {
	for i := range maxSize {
		size := 1 << i
		caches[i].New = func() any {
			buf := dirtymake.Bytes(0, size)
			return unsafe.SliceData(buf)
		}
	}
}

func Malloc(size int, capacity ...int) []byte {
	if len(capacity) > 1 {
		panic("too many arguments to Malloc")
	}
	var c = size
	if len(capacity) == 1 {
		c = capacity[0]
		if c < size {
			panic("capacity smaller than len")
		}
	}
	index := calcIndex(c)
	data := caches[index].Get().(*byte)
	return unsafe.Slice(data, 1<<index)[:size]
}

func calcIndex(size int) int {
	if size == 0 {
		return 0
	}
	if isPowerOfTwo(size) {
		return bsr(size)
	}
	return bsr(size) + 1
}

func Free(buf []byte) {
	size := cap(buf)
	if !isPowerOfTwo(size) { // ← 必须保留！非 2 的幂的 buffer 不能还池
		return
	}
	caches[bsr(size)].Put(unsafe.SliceData(buf))
}

func bsr(x int) int {
	return bits.Len(uint(x)) - 1
}

func isPowerOfTwo(x int) bool {
	return (x != 0) && ((x & (-x)) == x)
}
