// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type ReliableQueue struct {
	cl               *redis.Client
	name             string
	OnUnmarshalError func(err error)
	OnRedisError     func(err error)
}

func New(cl *redis.Client, name string) *ReliableQueue {
	q := &ReliableQueue{
		cl:               cl,
		name:             name,
		OnUnmarshalError: func(_ error) {},
		OnRedisError:     func(_ error) {},
	}
	return q
}

func (q *ReliableQueue) initCallbacks() {
	if q.OnUnmarshalError != nil {
		q.OnUnmarshalError = func(_ error) {}
	}
	if q.OnRedisError != nil {
		q.OnRedisError = func(_ error) {}
	}
}

func (q *ReliableQueue) Listen(ctx context.Context, workerID string) (ch <-chan Message, closefn func()) {
	q.initCallbacks()

	q.queuePastEvents(ctx, q.processingHashName(workerID))

	closectx, closefn := context.WithCancel(ctx)
	xch := make(chan Message) // unbuferred channel
	go q.waitForEvents(closectx, workerID, xch)
	go func() {
		<-closectx.Done()
		time.Sleep(time.Millisecond)
		close(xch)
	}()
	return xch, closefn
}

func (q *ReliableQueue) ListenSafe(ctx context.Context, workerID string) (ch <-chan SafeMessageChan, closefn func()) {
	q.initCallbacks()

	q.queuePastEvents(ctx, q.processingHashName(workerID))

	closectx, closefn := context.WithCancel(ctx)
	xch := make(chan SafeMessageChan) // unbuferred channel
	go q.waitForEventsSafe(closectx, q.processingHashName(workerID), xch)
	go func() {
		<-closectx.Done()
		time.Sleep(time.Millisecond)
		close(xch)
	}()
	return xch, closefn
}

// In case of crash of the worker, the processing queue might contain events not processed yet.
// This ensure events are re-queued on main event queue before going further
func (q *ReliableQueue) queuePastEvents(ctx context.Context, workerId string) {
	const count = 100
	var processingHash = q.processingHashName(workerId)
	for cursor := uint64(0); ; {
		var (
			result []string
			err    error
		)
		result, cursor, err = q.cl.HScan(ctx, processingHash, cursor, "*", count).Result()
		if err != nil {
			q.OnRedisError(err)
		}
		if len(result) == 0 {
			break
		}
		var pipe = q.cl.TxPipeline()
		for i := 0; i < len(result); i += 2 {
			key, value := result[i], result[i+1]
			if err := pipe.LPush(ctx, q.name, value).Err(); err != nil {
				q.OnRedisError(fmt.Errorf("LPush failed while queueing past events: %w", err))
				return
			}
			if err := pipe.HDel(ctx, processingHash, key).Err(); err != nil {
				q.OnRedisError(fmt.Errorf("HDel failed while queueing past events: %w", err))
				return
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			q.OnRedisError(fmt.Errorf("pipe.Exec failed while queueing past events: %w", err))
			return
		}
	}
}

func (q *ReliableQueue) processingHashName(workerID string) string {
	return q.name + "-processing-" + workerID
}

func (q *ReliableQueue) waitForEvents(ctx context.Context, workerId string, ch chan<- Message) {
	var processingHash = q.processingHashName(workerId)
	for ctx.Err() == nil {
		msgKey, msg, _ := q.popMessage(ctx, workerId)
		if msg == nil {
			continue
		}
		select {
		case ch <- *msg:
			// ok
		case <-ctx.Done():
			return
		}
		if err := q.cl.HDel(ctx, processingHash, msgKey).Err(); err != nil {
			q.OnRedisError(fmt.Errorf("HDel failed after message was sent: %w", err))
		}
	}
}

func (q *ReliableQueue) popMessage(ctx context.Context, workerId string) (key string, message *Message, rerr error) {
	var msg Message
	if err := q.cl.Watch(ctx, func(tx *redis.Tx) error {
		res, err := tx.BRPop(ctx, 0, q.name).Result()
		if err != nil {
			q.OnRedisError(fmt.Errorf("BRPop failed while popping message: %w", err))
			return err
		}
		if len(res) < 2 {
			return errors.New("redis: empty response")
		}
		var msgContent = res[1]
		if err := json.Unmarshal([]byte(msgContent), &msg); err != nil {
			if q.OnUnmarshalError != nil {
				q.OnUnmarshalError(err)
			}
			return err
		}
		key = uuid.NewString()
		if _, err := tx.HSet(ctx, q.processingHashName(workerId), key, msgContent).Result(); err != nil {
			return err
		}
		return nil
	}, q.name); err != nil {
		return "", nil, err
	}
	return key, &msg, nil
}

func (q *ReliableQueue) waitForEventsSafe(ctx context.Context, workerId string, ch chan<- SafeMessageChan) {
	var processingHash = q.processingHashName(workerId)
	for ctx.Err() == nil {
		msgKey, msg, err := q.popMessage(ctx, workerId)
		if err != nil {
			continue
		}
		var fnc = func(wrapper SafeMessageFn) {
			if wrapper(*msg) {
				if err := q.cl.HDel(ctx, processingHash, msgKey).Err(); err != nil {
					q.OnRedisError(fmt.Errorf("HDel failed after ack: %w", err))
				}
			}
		}
		select {
		case ch <- fnc:
			// ok
		case <-ctx.Done():
			return
		}
	}
}

type Message struct {
	Topic   string          `json:"topic"`
	Content json.RawMessage `json:"content"`
}

func (m Message) Empty() bool {
	return m.Topic == "" && len(m.Content) == 0
}

type SafeMessageFn func(msg Message) (ack bool)

type SafeMessageChan func(wrapper SafeMessageFn)

type inMessage struct {
	Topic   string      `json:"topic"`
	Content interface{} `json:"content"`
}

func (q *ReliableQueue) PushMessage(ctx context.Context, topic string, content interface{}) error {
	contentb, err := json.Marshal(inMessage{
		Topic:   topic,
		Content: content,
	})
	if err != nil {
		return fmt.Errorf("failed to JSON marshal message: %w", err)
	}
	return q.cl.LPush(ctx, q.name, string(contentb)).Err()
}
