package main

import (
	"context"
	"testing"
	"time"
)

func Test_Timer(t *testing.T) {
	t.Run("time.After", func(t *testing.T) {
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				break
			case <-time.After(time.Second):
			}
		}
	})
	t.Run("time.Sleep", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
		}
	})
}
