package gopool

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestCtxGo(t *testing.T) {
	wg := sync.WaitGroup{}
	for range 10 {
		wg.Add(1)
		CtxGo(context.Background(), func() {
			defer wg.Done()
			fmt.Println("hello world")
		})
	}
	wg.Wait()
}
