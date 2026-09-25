package mcache

import (
	"math/bits"
	"sync"
	"unsafe"

	"github.com/2comjie/ntool/lang/dirtymake"
)

const maxSize = 46

var caches [maxSize]sync.Pool

type bytesHeader struct {
	Data *byte
	Len  int
	Cap  int
}

func init() {
	for i := range maxSize {
		size := 1 << i
		caches[i].New = func() any {
			buf := dirtymake.Bytes(0, size)
			h := (*bytesHeader)(unsafe.Pointer(&buf))
			return h.Data
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
			panic("capacity small than len")
		}
	}

	index := calcIndex(c)
	var ret []byte
	header := (*bytesHeader)(unsafe.Pointer(&ret))
	header.Cap = 1 << index
	header.Len = size
	header.Data = caches[index].Get().(*byte)
	return ret
}

func calcIndex(size int) int {
	if size <= 0 {
		return 0
	}
	return bits.Len(uint(size - 1))
}

func Free(buf []byte) {
	size := cap(buf)
	index := calcIndex(size)
	header := (*bytesHeader)(unsafe.Pointer(&buf))
	caches[index].Put(header.Data)
}
