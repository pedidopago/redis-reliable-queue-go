package rq

import (
	"context"
	"encoding/json"
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

	assert.NoError(t, rq.PushMessage(context.Background(), "default_topic", "test message"))
	msgr := <-ch
	assert.Equal(t, "default_topic", msgr.Topic)
	str := ""
	assert.NoError(t, json.Unmarshal(msgr.Content, &str))
	assert.Equal(t, "test message", str)
}
