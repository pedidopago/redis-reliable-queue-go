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

func TestReliableQueue(t *testing.T) {

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cl := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue",
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	assert.NoError(t, q.PushMessage(context.Background(), "test message 1"))
	assert.NoError(t, q.PushMessage(context.Background(), "test message 2 | | | "))
	assert.NoError(t, q.PushMessage(context.Background(), "test message 3"))

	rmap := sync.Map{}
	rmap.Store("test message 1", 0)
	rmap.Store("test message 2 | | | ", 0)
	rmap.Store("test message 3", 0)

	ch0 := make(chan struct{})
	wg := sync.WaitGroup{}

	fn1 := func() error {
		defer wg.Done()
		<-ch0
		return q.PopMessage(context.Background(), func(msg string) error {
			vv, ok := rmap.Load(msg)
			if !ok {
				t.Fail()
				return nil
			}
			v := vv.(int)
			v++
			rmap.Store(msg, v)
			return nil
		})
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			_ = fn1()
		}()
	}

	close(ch0) // will make all goroutines run at the same time
	wg.Wait()

	v1, _ := rmap.Load("test message 1")
	v2, _ := rmap.Load("test message 2 | | | ")
	v3, _ := rmap.Load("test message 3")
	v4, _ := rmap.Load("test message")

	assert.Equal(t, 1, v1.(int))
	assert.Equal(t, 1, v2.(int))
	assert.Equal(t, 1, v3.(int))
	assert.Nil(t, v4)
}
