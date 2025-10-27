// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
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

func (q *ReliableQueue) Listen(ctx context.Context, workerID string) (ch <-chan *Message, closefn func()) {
	q.queuePastEvents(ctx, q.processingQueueName(workerID))

	closectx, closefn := context.WithCancel(ctx)
	xch := make(chan *Message) // unbuferred channel
	go q.waitForEvents(closectx, q.processingQueueName(workerID), xch)
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

func (q *ReliableQueue) processingQueueName(workerID string) string {
	return q.name + "-processing-" + workerID
}

func (q *ReliableQueue) waitForEvents(ctx context.Context, processingQueue string, ch chan<- *Message) {
	for ctx.Err() == nil {
		msg := q.cl.BRPopLPush(ctx, q.name, processingQueue, 0).Val()
		if msg != "" {
			msgx := &Message{
				Payload: msg,
				Consume: func() error {
					return q.cl.LRem(ctx, processingQueue, 1, msg).Err()
				},
			}
			select {
			case ch <- msgx:
				// ok
			case <-ctx.Done():
				return
			}
		}
	}
}

type Message struct {
	// Payload is the raw content of the message.
	Payload string
	// Consume will remove this message from the "worker_id" list. You
	// must call this function exactly ONCE after the message was processed
	// successfully from the application.
	Consume func() error
}

func (q *ReliableQueue) PushMessage(ctx context.Context, payload string) error {
	return q.cl.LPush(ctx, q.name, payload).Err()
}
