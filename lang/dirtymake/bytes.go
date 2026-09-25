//go:build !tinygo

package dirtymake

import "unsafe"

// 正常写 make([]byte, n) 编译器会做两件事：调用 runtime 分配内存 + 把内存清零
// 这里我们直接告诉编译器 别清零 然后手工拼出一个 []byte 返回
// make([]byte, n)        →  runtime.mallocgc(size, type, needzero=true)   ← 编译器包办
// dirtmake.Bytes(n, n)   →  runtime.mallocgc(size, nil,  needzero=false)  ← 手工直调
// 省的就是那一次全量清零（memclr）

//go:linkname mallocgc runtime.mallocgc
func mallocgc(size uintptr, typ unsafe.Pointer, needzero bool) unsafe.Pointer

func Bytes(len, cap int) (b []byte) {
	if len < 0 || len > cap {
		panic("dirtmake.Bytes: len out of range")
	}
	p := mallocgc(uintptr(cap), nil, false) // needzero 传入false 不清零内存
	b = unsafe.Slice((*byte)(p), cap)[:len]
	return
}
