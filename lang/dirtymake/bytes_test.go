package dirtymake

import (
	"fmt"
	"testing"
)

func TestMalloc(t *testing.T) {
	b := Bytes(8, 24)
	fmt.Printf("len %d cap %d \n", len(b), cap(b))
}
