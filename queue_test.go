package rq

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func testSetupRedis() *redis.Client {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	cl := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    os.Getenv("TEST_REDIS_PASSWORD"),
		DialTimeout: time.Second * 15,
	})

	return cl
}

func TestReliableQueue(t *testing.T) {

	cl := testSetupRedis()
	defer cl.Close()

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

func TestAck(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*20)
	defer cf()

	cl.RPush(ctx, "microservices_tests_redis_reliable_queue_ackers-ack", "0|bacon", "0|salad")

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue_ackers",
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	q.RestoreExpiredMessages(ctx, 0)

	it0 := cl.LRange(ctx, "microservices_tests_redis_reliable_queue_ackers", 0, -1).Val()

	assert.Equal(t, 2, len(it0))

	cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers")
}

func TestAutoAckRecover(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*20)
	defer cf()

	tn0 := strconv.FormatInt(time.Now().Add(time.Minute*5).Unix(), 10)
	unremovable := tn0 + "|shouldnotremove"

	cl.RPush(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack", "0|bacon", "0|salad", unremovable)

	defer cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack")
	defer cl.Del(ctx, "microservices_tests_redis_reliable_queue_ackers_auto")

	q := Queue{
		RedisClient:           cl,
		Name:                  "microservices_tests_redis_reliable_queue_ackers_auto",
		MessageExpiration:     time.Minute,
		ListExpirationSeconds: "3600",
	}

	assert.NoError(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "bacon", msg)
		return nil
	}))

	assert.NoError(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "salad", msg)
		return nil
	}))

	assert.Error(t, q.PopMessage(ctx, func(msg string) error {
		assert.Equal(t, "", msg)
		return nil
	}))

	newLen := cl.LLen(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack").Val()
	if !assert.Equal(t, int64(1), newLen) {
		slc := cl.LRange(ctx, "microservices_tests_redis_reliable_queue_ackers_auto-ack", 0, -1).Val()
		for _, v := range slc {
			t.Log(v)
		}
	}
}

func TestPopMessageWithAck(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*20)
	defer cf()

	const qname = "microservices_tests_redis_reliable_queue_pop_with_ack"
	cl.Del(ctx, qname, qname+"-ack")
	defer cl.Del(ctx, qname, qname+"-ack")

	q := Queue{
		RedisClient:           cl,
		Name:                  qname,
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	assert.NoError(t, q.PushMessage(ctx, "with ack 1"))
	assert.NoError(t, q.PushMessage(ctx, "with ack 2"))

	// pop + ack: the message must leave the ack list
	msg, ack, err := q.PopMessageWithAck(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "with ack 1", msg)
	assert.Equal(t, int64(1), cl.LLen(ctx, qname+"-ack").Val())
	ack()
	assert.Equal(t, int64(0), cl.LLen(ctx, qname+"-ack").Val())

	// pop without ack: the message must stay on the ack list for redelivery
	msg, _, err = q.PopMessageWithAck(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "with ack 2", msg)
	assert.Equal(t, int64(1), cl.LLen(ctx, qname+"-ack").Val())

	// empty queue: error must be recognizable, ack must be safe to call
	msg, ack, err = q.PopMessageWithAck(ctx)
	assert.True(t, IsEmptyQueueError(err))
	assert.Equal(t, "", msg)
	assert.NotNil(t, ack)
	ack()

	// the unacked message is still there, untouched by the empty pop
	assert.Equal(t, int64(1), cl.LLen(ctx, qname+"-ack").Val())
}

// TestChannelClosesOnCancelWithTraffic exercises the shutdown path on a queue
// that never goes idle: with messages always available, the goroutine used to
// exit through its own loop condition without ever closing the channel,
// blocking `for msg := range ch` consumers forever.
func TestChannelClosesOnCancelWithTraffic(t *testing.T) {
	cl := testSetupRedis()
	defer cl.Close()

	const qname = "microservices_tests_redis_reliable_queue_channel_close"
	cleanupCtx := context.Background()
	cl.Del(cleanupCtx, qname, qname+"-ack")
	defer cl.Del(cleanupCtx, qname, qname+"-ack")

	q := Queue{
		RedisClient:           cl,
		Name:                  qname,
		MessageExpiration:     time.Minute * 5,
		ListExpirationSeconds: "3600",
	}

	// enough traffic that the goroutine never reaches an idle branch
	for i := 0; i < 500; i++ {
		assert.NoError(t, q.PushMessage(cleanupCtx, "traffic "+strconv.Itoa(i)))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := q.Channel(ctx)

	// consume a few messages so the pop loop is demonstrably active
	for i := 0; i < 3; i++ {
		msg := <-ch
		assert.NoError(t, msg.Err)
		msg.AckMessage()
	}

	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range ch {
			if msg.Err == nil {
				msg.AckMessage()
			}
		}
	}()

	select {
	case <-done:
		// channel closed: range loop terminated as it must
	case <-time.After(time.Second * 5):
		t.Fatal("Channel(ctx) was not closed after context cancellation on a busy queue")
	}
}
