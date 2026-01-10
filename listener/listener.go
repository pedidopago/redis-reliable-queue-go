package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	rq "github.com/pedidopago/redis-reliable-queue-go"
)

type HandlerFn[T any] func(context.Context, T) error

type ErrorDetails struct {
	Cause       error
	Topic       string
	RawMessage  json.RawMessage
	Description string
}

type Listener struct {
	queueName            string
	errorQueueName       string
	workedID             string
	redis                *redis.Client
	resetRetriesInterval time.Duration
	topicHandlerRegistry map[string]func(context.Context, json.RawMessage) error

	// Callbacks

	OnMessageReceived func(l *Listener, message rq.Message)
	OnUnhandledTopic  func(l *Listener, topic string)
	OnUnmarshalError  func(l *Listener, details *ErrorDetails)
	OnRedisError      func(l *Listener, details *ErrorDetails)
	OnHandlerError    func(l *Listener, details *ErrorDetails)
	OnRetriesExceeded func(l *Listener, details *ErrorDetails)
}

func (l *Listener) GetQueueName() string {
	return l.queueName
}

func (l *Listener) GetErrorQueueName() string {
	return l.errorQueueName
}

func (l *Listener) GetWorkerID() string {
	return l.workedID
}

func (l *Listener) initCallbacks() {
	if l.OnMessageReceived == nil {
		l.OnMessageReceived = func(_ *Listener, _ rq.Message) {}
	}
	if l.OnUnhandledTopic == nil {
		l.OnUnhandledTopic = func(_ *Listener, _ string) {}
	}
	if l.OnUnmarshalError == nil {
		l.OnUnmarshalError = func(_ *Listener, _ *ErrorDetails) {}
	}
	if l.OnRedisError == nil {
		l.OnRedisError = func(_ *Listener, _ *ErrorDetails) {}
	}
	if l.OnHandlerError == nil {
		l.OnHandlerError = func(_ *Listener, _ *ErrorDetails) {}
	}
	if l.OnRetriesExceeded == nil {
		l.OnRetriesExceeded = func(_ *Listener, _ *ErrorDetails) {}
	}
}

func (l *Listener) reprocessErrorEvents(ctx context.Context) {
	for {
		if err := l.redis.RPopLPush(ctx, l.errorQueueName, l.queueName).Err(); err != nil {
			if err != redis.Nil {
				l.OnRedisError(l, &ErrorDetails{
					Cause:       err,
					Description: "RPopLPush failed",
				})
			}
			break
		}
	}
}

func NewListener(redis *redis.Client, queue, errorQueue, workerID string, resetRetriesInterval time.Duration) *Listener {
	return &Listener{
		queueName:            queue,
		errorQueueName:       errorQueue,
		workedID:             workerID,
		redis:                redis,
		resetRetriesInterval: resetRetriesInterval,
		topicHandlerRegistry: make(map[string]func(context.Context, json.RawMessage) error),
	}
}

const globalHandlerKey = "*"

// RegisterHandler will register a global handler
func RegisterHandler[T any](l *Listener, handler HandlerFn[T]) {
	RegisterTopicHandler(l, globalHandlerKey, handler)
}

// RegisterTopicHandler will register a handler for the topic
func RegisterTopicHandler[T any](l *Listener, topic string, handler HandlerFn[T]) {
	if l.topicHandlerRegistry == nil {
		l.topicHandlerRegistry = make(map[string]func(context.Context, json.RawMessage) error)
	}
	l.topicHandlerRegistry[topic] = func(ctx context.Context, m json.RawMessage) error {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			l.OnUnmarshalError(l, &ErrorDetails{
				Cause:      err,
				Topic:      topic,
				RawMessage: m,
			})
			return err
		}
		return handler(ctx, v)
	}
}

type INumberOfRetries interface {
	GetNumberOfRetries() uint8
	IncrementNumberOfRetries()
	ResetNumberOfRetries()
}

type NumberOfRetries struct {
	NumberOfRetries uint8 `json:"number_of_retries,omitempty"`
}

func (x NumberOfRetries) GetNumberOfRetries() uint8 {
	return x.NumberOfRetries
}

func (x *NumberOfRetries) IncrementNumberOfRetries() {
	x.NumberOfRetries++
}

func (x *NumberOfRetries) ResetNumberOfRetries() {
	x.NumberOfRetries = 0
}

// RegisterHandlerWithRetry will register a global handler using retry
func RegisterHandlerWithRetry[T INumberOfRetries](l *Listener, maxRetries uint8, handler HandlerFn[T]) {
	RegisterTopicHandlerWithRetry(l, globalHandlerKey, maxRetries, handler)
}

// RegisterTopicHandlerWithRetry will register a handler for the topic using retry
func RegisterTopicHandlerWithRetry[T INumberOfRetries](l *Listener, topic string, maxRetries uint8, handler HandlerFn[T]) {
	l.topicHandlerRegistry[topic] = func(ctx context.Context, m json.RawMessage) error {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			l.OnUnmarshalError(l, &ErrorDetails{
				Cause:      err,
				Topic:      topic,
				RawMessage: m,
			})
			return err
		}
		if err := handler(ctx, v); err != nil {
			l.OnHandlerError(l, &ErrorDetails{
				Cause:      err,
				Topic:      topic,
				RawMessage: m,
			})
			if v.GetNumberOfRetries() >= maxRetries {
				l.OnRetriesExceeded(l, &ErrorDetails{
					Cause:       err,
					Topic:       topic,
					RawMessage:  m,
					Description: fmt.Sprintf("max number of retries exceeded (%d / %d)", v.GetNumberOfRetries(), maxRetries),
				})
				v.ResetNumberOfRetries()
				if err := rq.New(l.redis, l.errorQueueName).PushMessage(ctx, topic, v); err != nil {
					l.OnRedisError(l, &ErrorDetails{
						Cause:       err,
						Topic:       topic,
						RawMessage:  m,
						Description: "redis.PushMessage into error queue failed",
					})
					return err
				}
				return nil
			}
			v.IncrementNumberOfRetries()
			if serr := rq.New(l.redis, l.queueName).PushMessage(ctx, topic, v); serr != nil {
				l.OnRedisError(l, &ErrorDetails{
					Cause:       serr,
					Topic:       topic,
					RawMessage:  m,
					Description: "redis.PushMessage into queue failed",
				})
				return serr
			}
		}
		return nil
	}
}

func setupWorkers(n uint8) chan any {
	if n < 1 {
		n = 1
	}
	var workersChan = make(chan any, n)
	for i := 0; i < cap(workersChan); i++ {
		workersChan <- nil
	}
	return workersChan
}

func listen(ctx context.Context, queue *rq.ReliableQueue, workerID string, callback rq.SafeMessageFn, workers uint8) {
	var workersChan = setupWorkers(workers)

	ch, cl := queue.ListenSafe(ctx, workerID)
	defer cl()

	for fn := range ch {
		<-workersChan
		go func(fn rq.SafeMessageChan) {
			defer func() { workersChan <- nil }()
			fn(callback)
		}(fn)
	}
}

func (l *Listener) Listen(ctx context.Context, redisCl *redis.Client, workers uint8) {
	l.initCallbacks()

	if l.errorQueueName != "" && l.resetRetriesInterval != 0 {
		go func() {
			timer := time.NewTimer(0)
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					l.reprocessErrorEvents(ctx)
					timer.Reset(l.resetRetriesInterval)
				}
			}
		}()
	}

	queue := rq.New(redisCl, l.queueName)
	queue.OnRedisQueueError = func(name string, err error) {
		l.OnRedisError(l, &ErrorDetails{
			Cause:       err,
			Description: fmt.Sprintf("error in queue %s", name),
		})
	}
	listen(ctx, queue, l.workedID, func(msg rq.Message) (ack bool) {
		l.OnMessageReceived(l, msg)
		handlerFn := l.topicHandlerRegistry[msg.Topic]
		if handlerFn == nil {
			// try to get global
			handlerFn = l.topicHandlerRegistry[globalHandlerKey]
			if handlerFn == nil {
				l.OnUnhandledTopic(l, msg.Topic)
				return true
			}
		}
		if err := handlerFn(ctx, msg.Content); err != nil {
			l.OnHandlerError(l, &ErrorDetails{
				Cause:      err,
				Topic:      msg.Topic,
				RawMessage: msg.Content,
			})
			return false
		}
		return true
	}, workers)
}
