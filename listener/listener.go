package listener

import (
	"context"
	"encoding/json"
	"errors"
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

type topicRegistryEntry struct {
	unmarshaler  func(json.RawMessage) (any, error)
	interceptors []func(context.Context, json.RawMessage, any) error
	handler      func(context.Context, json.RawMessage, any) error
}

type Listener struct {
	queueName            string
	errorQueueName       string
	workedID             string
	redis                *redis.Client
	resetRetriesInterval time.Duration
	topicRegistry        map[string]topicRegistryEntry

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
			if !errors.Is(err, redis.Nil) {
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
		topicRegistry:        make(map[string]topicRegistryEntry),
	}
}

const globalHandlerKey = "*"

// RegisterHandler will register a global handler
func RegisterHandler[T any](l *Listener, handler HandlerFn[T]) {
	RegisterTopicHandler(l, globalHandlerKey, handler)
}

// RegisterTopicHandler will register a handler for the topic
func RegisterTopicHandler[T any](l *Listener, topic string, handler HandlerFn[T], interceptors ...HandlerFn[T]) {
	if l.topicRegistry == nil {
		l.topicRegistry = make(map[string]topicRegistryEntry)
	}
	var entry topicRegistryEntry
	entry.unmarshaler = func(m json.RawMessage) (any, error) {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			l.OnUnmarshalError(l, &ErrorDetails{
				Cause:      err,
				Topic:      topic,
				RawMessage: m,
			})
			return nil, err
		}
		return v, nil
	}
	for _, interceptorFn := range interceptors {
		entry.interceptors = append(entry.interceptors, func(ctx context.Context, _ json.RawMessage, a any) error {
			return interceptorFn(ctx, a.(T))
		})
	}
	entry.handler = func(ctx context.Context, _ json.RawMessage, a any) error {
		return handler(ctx, a.(T))
	}
	l.topicRegistry[topic] = entry
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

func errorHandler[T INumberOfRetries](
	l *Listener,
	ctx context.Context,
	maxRetries uint8,
	err error,
	topic string,
	m json.RawMessage,
	v T,
) error {
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
		if l.errorQueueName != "" && l.resetRetriesInterval != 0 {
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
	return nil
}

// RegisterTopicHandlerWithRetry will register a handler for the topic using retry
func RegisterTopicHandlerWithRetry[T INumberOfRetries](l *Listener, topic string, maxRetries uint8, handler HandlerFn[T], interceptors ...HandlerFn[T]) {
	var entry topicRegistryEntry
	entry.unmarshaler = func(m json.RawMessage) (any, error) {
		var v T
		if err := json.Unmarshal(m, &v); err != nil {
			l.OnUnmarshalError(l, &ErrorDetails{
				Cause:      err,
				Topic:      topic,
				RawMessage: m,
			})
			return nil, err
		}
		return v, nil
	}
	for _, interceptorFn := range interceptors {
		entry.interceptors = append(entry.interceptors, func(ctx context.Context, m json.RawMessage, a any) error {
			var v = a.(T)
			if err := interceptorFn(ctx, v); err != nil {
				return errorHandler(l, ctx, maxRetries, err, topic, m, v)
			}
			return nil
		})
	}
	entry.handler = func(ctx context.Context, m json.RawMessage, a any) error {
		var v = a.(T)
		if err := handler(ctx, v); err != nil {
			return errorHandler(l, ctx, maxRetries, err, topic, m, v)
		}
		return nil
	}
	l.topicRegistry[topic] = entry
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
	queue.OnRedisError = func(err error) {
		l.OnRedisError(l, &ErrorDetails{
			Cause:       err,
			Description: fmt.Sprintf("redis error in queue %s", l.queueName),
		})
	}
	queue.OnUnmarshalError = func(err error) {
		l.OnUnmarshalError(l, &ErrorDetails{
			Cause:       err,
			Description: fmt.Sprintf("unmarshal error in queue %s", l.queueName),
		})
	}

	var workersChan = setupWorkers(workers)

	ch, cl := queue.ListenSafe(ctx, l.workedID)
	defer cl()

	for fn := range ch {
		<-workersChan
		func(fn rq.SafeMessageChan) {
			fn(func(msg rq.Message, ack chan<- bool) {
				l.OnMessageReceived(l, msg)
				entry, ok := l.topicRegistry[msg.Topic]
				if !ok {
					// try to get global
					entry, ok = l.topicRegistry[globalHandlerKey]
					if !ok {
						l.OnUnhandledTopic(l, msg.Topic)
						ack <- false
						return
					}
				}

				v, err := entry.unmarshaler(msg.Content)
				if err != nil {
					ack <- false
					return
				}

				for _, interceptorFn := range entry.interceptors {
					if err := interceptorFn(ctx, msg.Content, v); err != nil {
						ack <- false
						return
					}
				}

				go func() {
					defer func() { workersChan <- nil }()
					var success bool
					defer func() { ack <- success }()
					err := entry.handler(ctx, msg.Content, v)
					success = err == nil
				}()
			})
		}(fn)
	}
}
