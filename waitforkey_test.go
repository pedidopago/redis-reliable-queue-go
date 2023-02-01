package rq

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestWaitForKey(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cl := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	gotkey := false
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ctx, cf := context.WithTimeout(context.Background(), time.Second*5)
		defer cf()
		val, err := WaitForKey(ctx, cl, "testkey-1292934")
		assert.NoError(t, err)
		assert.Equal(t, "testvalue", val)
		gotkey = true
	}()

	time.Sleep(time.Second * 1)
	cl.Set(context.Background(), "testkey-1292934", "testvalue", time.Minute*5)
	wg.Wait()
	assert.True(t, gotkey)
}
