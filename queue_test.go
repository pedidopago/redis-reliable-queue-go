package rq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestReliableQueue(t *testing.T) {

	cl := redis.NewClient(&redis.Options{
		Addr:        os.Getenv("TEST_REDIS_ADDR"),
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	rq := New(cl, "microservices_tests_redis_reliable_queue")

	ch, closefn := rq.Listen(context.Background(), "worker1")
	defer closefn()

	assert.NoError(t, rq.PushMessage(context.Background(), "test message"))
	msgr := <-ch
	assert.NotNil(t, msgr)
	assert.Equal(t, "test message", msgr.Payload)
	assert.NoError(t, msgr.Consume())
}
