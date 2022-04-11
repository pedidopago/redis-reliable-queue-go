// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type ReliableQueue struct {
	cl                *redis.Client
	name              string
	OnUnmarshalError  func(err error)
	OnRedisQueueError func(name string, err error)
}

func New(cl *redis.Client, name string) *ReliableQueue {
	q := &ReliableQueue{
		cl:   cl,
		name: name,
	}
	return q
}

func (q *ReliableQueue) Listen(ctx context.Context, workerID string) (ch <-chan Message, closefn func()) {
	processingQueue := q.name + "-processing-" + workerID
	_ = processingQueue
	q.queuePastEvents(ctx, processingQueue)

	closectx, closefn := context.WithCancel(ctx)
	xch := make(chan Message) // unbuferred channel
	go q.waitForEvents(closectx, processingQueue, xch)
	go func() {
		<-closectx.Done()
		time.Sleep(time.Millisecond)
		close(xch)
	}()
	return xch, closefn
}

// In case of crash of the worker, the processing queue might contain events not processed yet.
// This ensure events are re-queued on main event queue before going further
func (q *ReliableQueue) queuePastEvents(ctx context.Context, processingQueue string) {
	for {
		if v := q.cl.RPopLPush(ctx, processingQueue, q.name).Val(); v == "" {
			break
		}
	}
}

func (q *ReliableQueue) waitForEvents(ctx context.Context, processingQueue string, ch chan<- Message) {
	for ctx.Err() == nil {
		msg := q.cl.BRPopLPush(ctx, q.name, processingQueue, 0).Val()
		if msg != "" {
			msgx := &Message{}
			if err := json.Unmarshal([]byte(msg), msgx); err != nil {
				if q.OnUnmarshalError != nil {
					q.OnUnmarshalError(err)
				}
			} else {
				select {
				case ch <- *msgx:
					// ok
				case <-ctx.Done():
					return
				}
			}
			if err := q.cl.LRem(ctx, processingQueue, 1, msg).Err(); err != nil {
				if q.OnRedisQueueError != nil {
					q.OnRedisQueueError(processingQueue, err)
				}
			}
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
