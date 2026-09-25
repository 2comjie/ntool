package dirtymake

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestSliceHeader(t *testing.T) {
	// 切片头
	b := Bytes(1024*1024*8, 1024*1024*8)

	header := (*slice)(unsafe.Pointer(&b))
	fmt.Printf("cap %d len %d %p %p %p %v\n", header.cap, header.len, header.data, b, header, b)
}

func TestMalloc(t *testing.T) {
	b := Bytes(1024*1024*8, 1024*1024*8)
	header := (*slice)(unsafe.Pointer(&b))
	fmt.Printf("cap %d len %d %p %p %p %v\n", header.cap, header.len, header.data, b, header, b)
}
