package mcache

import (
	"fmt"
	"testing"
)

func TestMcache(t *testing.T) {
	b := Malloc(10)
	for i := range 10 {
		b[i] = byte('0' + i)
	}
	fmt.Println(b)
	Free(b)
	b = Malloc(10)
	fmt.Println(b)
}
