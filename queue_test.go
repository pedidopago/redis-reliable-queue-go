package rq

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestReliableQueueSafe(t *testing.T) {
	cl := redis.NewClient(&redis.Options{
		Addr:        os.Getenv("TEST_REDIS_ADDR"),
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	rq := New(cl, "microservices_tests_redis_reliable_queue_safe")

	ch, closefn := rq.ListenSafe(context.Background(), "worker1")

	// push 3 items
	assert.NoError(t, rq.PushMessage(context.Background(), "default_topic", "1"))
	assert.NoError(t, rq.PushMessage(context.Background(), "default_topic", "2"))
	assert.NoError(t, rq.PushMessage(context.Background(), "default_topic", "3"))

	// get item 1 (and mark as ok)
	msgh := <-ch
	msgh(func(msg Message) error {
		// if no error is returned, this item is safelly processed (and removed from the pending queue)
		assert.Equal(t, "default_topic", msg.Topic)
		var value string
		assert.NoError(t, json.Unmarshal(msg.Content, &value))
		assert.Equal(t, "1", value)
		return nil
	})

	// get item 2 and fail (after processing item 3)
	msgh2 := <-ch
	// get item 3 (and mark as ok)
	msgh3 := <-ch
	msgh3(func(msg Message) error {
		// if no error is returned, this item is safelly processed (and removed from the pending queue)
		assert.Equal(t, "default_topic", msg.Topic)
		var value string
		assert.NoError(t, json.Unmarshal(msg.Content, &value))
		assert.Equal(t, "3", value)
		return nil
	})
	// fail item 2
	msgh2(func(msg Message) error {
		// if an error is returned, this item is re-added to the queue
		assert.Equal(t, "default_topic", msg.Topic)
		var value string
		assert.NoError(t, json.Unmarshal(msg.Content, &value))
		assert.Equal(t, "2", value)
		return fmt.Errorf("failing to reschedule item 2")
	})

	// at this step, the program has "crashed"
	closefn()

	rq = New(cl, "microservices_tests_redis_reliable_queue_safe")

	ch, closefn = rq.ListenSafe(context.Background(), "worker1")
	defer closefn()

	// get item 2 again
	msgh2 = <-ch
	msgh2(func(msg Message) error {
		assert.Equal(t, "default_topic", msg.Topic)
		var value string
		assert.NoError(t, json.Unmarshal(msg.Content, &value))
		assert.Equal(t, "2", value)
		return nil
	})
}
