// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	//go:embed queue.lua
	luaScript string
)

const (
	DefaultAckLimit = 10000
)

// ErrAckEntryGone is returned by PopMessageWithAck's ack when the entry it
// would remove is no longer on the ack list. It is terminal: do not retry.
// The entry string cannot reappear, so a retry could only remove another
// consumer's byte-identical entry.
var ErrAckEntryGone = errors.New("ack entry is no longer on the ack list")

type Queue struct {
	RedisClient           *redis.Client
	Name                  string
	AckSuffix             string
	AckLimit              int
	LeftPush              bool
	MessageExpiration     time.Duration // default 20m
	ListExpirationSeconds string        // default "3600" (1h)
}

func (q Queue) PushMessage(ctx context.Context, data string) error {
	if q.LeftPush {
		return q.RedisClient.LPush(ctx, q.Name, data).Err()
	}
	return q.RedisClient.RPush(ctx, q.Name, data).Err()
}

func (q Queue) getSuffix() string {
	suffix := "-ack"
	if q.AckSuffix != "" {
		suffix = q.AckSuffix
	}
	return suffix
}

func (q Queue) getAckList() string {
	return q.Name + q.getSuffix()
}

func (q Queue) getListExpiration() string {
	lt := "3600"
	if q.ListExpirationSeconds != "" {
		lt = q.ListExpirationSeconds
	}
	return lt
}

func (q Queue) getPopCommand() string {
	popcommand := "lpop"
	if q.LeftPush {
		popcommand = "rpop"
	}

	return popcommand
}

func (q Queue) getMessageTimeout() time.Duration {
	mtimeout := time.Minute * 20

	if q.MessageExpiration != 0 {
		mtimeout = q.MessageExpiration
	}

	return mtimeout
}

func (q Queue) noop() {}

// doPopEvalEntry runs the pop script and returns the raw ack-list entry rather
// than a closure, so each caller decides the ack's context and lifetime. An
// empty ackEntry means the queue had nothing to hand out.
func (q Queue) doPopEvalEntry(ctx context.Context) (result string, ackEntry string, err error) {
	ackList := q.getAckList()
	popcommand := q.getPopCommand()
	mtimeout := q.getMessageTimeout()
	tnow := time.Now()
	tnowString := strconv.FormatInt(tnow.Unix(), 10)
	t1 := tnow.Add(mtimeout).Unix()
	t1s := strconv.FormatInt(t1, 10)
	ackListLimit := strconv.Itoa(nonzero(q.AckLimit, DefaultAckLimit))

	iresult, err := q.RedisClient.Eval(ctx, luaScript, []string{q.Name, ackList, tnowString, t1s, q.getListExpiration(), popcommand, ackListLimit}).Result()
	if err != nil {
		return "", "", err
	}
	if iresult == nil {
		return "", "", nil
	}
	result, ok := iresult.(string)
	if !ok {
		return "", "", fmt.Errorf("result is not a string")
	}

	return result, t1s + "|" + result, nil
}

func (q Queue) doPopEval(ctx context.Context) (result string, removefn func(), err error) {
	result, ackEntry, err := q.doPopEvalEntry(ctx)
	if err != nil || ackEntry == "" {
		return result, q.noop, err
	}

	ackList := q.getAckList()

	removefn = func() {
		if err := q.RedisClient.LRem(ctx, ackList, 1, ackEntry).Err(); err != nil {
			fmt.Println("error removing ack message", err)
		}
	}

	return result, removefn, nil
}

// noopAck is the ack handed back when there is nothing to acknowledge.
func noopAck(context.Context) error { return nil }

// PopMessageWithAck pops a single message and returns it together with an ack
// function. Unlike PopMessage, nothing is acknowledged automatically: the
// caller owns the ack and calls it once it decides the message was processed.
// Not calling ack leaves the message on the ack list to be redelivered after
// MessageExpiration -- the intended at-least-once behaviour for consumers that
// fail mid-processing. That guarantee has one hole, pre-dating this function:
// queue.lua trims the ack list right after pushing, keeping the OLDEST
// AckLimit+1 entries (ltrim's bounds are inclusive), so once the list is at the
// limit a just-popped message is trimmed off immediately and never redelivered.
// The trim runs only on the main-list pop path, so the list can drift past the
// limit via the expiry re-stamp.
//
// ack takes its OWN context rather than closing over this call's. Acking
// happens after processing, which is exactly when the pop's context is most
// likely to already be cancelled -- a shutdown, a request timeout -- and an ack
// bound to a dead context silently does nothing, so the message is reprocessed
// after MessageExpiration. Pass a live context. ack also returns the error
// instead of swallowing it, so a failed acknowledgement is visible.
//
// ack is safe to call more than once: it is a no-op after a CONFIRMED success,
// and retryable while it has not succeeded -- except for ErrAckEntryGone, which
// is terminal. The confirmation matters: if the LRem reaches Redis and executes
// but its reply is lost, ack reports failure while the removal happened, and a
// retry can remove another consumer's byte-identical entry. That ambiguity is
// not solvable with a remove-by-value, so treat a retried ack as best effort. That matters because ack-list entries are
// "<expiry>|<payload>", so two identical payloads popped within the same second
// are byte-identical -- a second removal would delete another consumer's
// in-flight entry.
//
// When the queue is empty the error satisfies IsEmptyQueueError. ack is never
// nil.
func (q Queue) PopMessageWithAck(ctx context.Context) (msg string, ack func(context.Context) error, err error) {
	result, ackEntry, err := q.doPopEvalEntry(ctx)
	if err != nil {
		return "", noopAck, err
	}
	if ackEntry == "" {
		return result, noopAck, nil
	}

	ackList := q.getAckList()

	var mu sync.Mutex
	acked := false

	ack = func(ackCtx context.Context) error {
		mu.Lock()
		defer mu.Unlock()

		if acked {
			return nil
		}
		removed, err := q.RedisClient.LRem(ackCtx, ackList, 1, ackEntry).Result()
		if err != nil {
			return err
		}
		if removed == 0 {
			// Nothing matched: the entry is gone. MessageExpiration elapsed and
			// queue.lua re-stamped it for another consumer, or the ack list hit
			// AckLimit and it was trimmed away, or the list's own TTL expired.
			// Reporting success would record an acknowledgement for a message
			// that may be in flight elsewhere, so say so instead.
			//
			// Latched as acked even though it failed, because this failure is
			// terminal: the entry string can never reappear (the re-stamp
			// writes a new expiry prefix). A retry could only match a
			// byte-identical entry belonging to whoever else popped the same
			// payload in the same second, and stealing that would leave THEIR
			// message unredeliverable.
			acked = true

			return ErrAckEntryGone
		}
		acked = true

		return nil
	}

	return result, ack, nil
}

func (q Queue) PopMessage(ctx context.Context, fn func(msg string) error) error {
	result, removefn, err := q.doPopEval(ctx)

	if err != nil {
		return err
	}

	if result == "" {
		return nil
	}

	err = fn(result)

	if err != nil {
		return err
	}

	removefn()

	return nil
}

type ChannelMessage struct {
	Message    string
	Err        error
	AckMessage func()
}

// deliver sends m on ch, preferring the buffer: only when the buffer is full
// does it race the cancellation. A single select would choose uniformly between
// a ready send and a done context, so cancelling would drop roughly half the
// in-hand messages even with a consumer still draining, each costing a full
// MessageExpiration before redelivery.
//
// Reports whether the producer should keep running. Abandoning a message leaves
// it unacked, so it is redelivered after MessageExpiration.
func deliver(ctx context.Context, ch chan<- *ChannelMessage, m *ChannelMessage) bool {
	select {
	case ch <- m:
		return true
	default:
	}

	select {
	case ch <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

func (q Queue) Channel(ctx context.Context) (channel <-chan *ChannelMessage) {
	ch := make(chan *ChannelMessage, 256)

	go func() {
		// Close on every exit path. Closing only from the two idle branches
		// left ch open forever whenever the loop exited at `for ctx.Err() ==
		// nil` instead -- the guaranteed path on a queue with traffic -- so
		// `for msg := range ch` never returned. Under a sync.WaitGroup that
		// turns SIGTERM into a hang until the process is killed.
		defer close(ch)

		for ctx.Err() == nil {
			result, removefn, err := q.doPopEval(ctx)

			if err != nil {

				if err == redis.Nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Millisecond * 100):
						// noop
					}
					continue
				}

				if !deliver(ctx, ch, &ChannelMessage{Err: err, AckMessage: q.noop}) {
					return
				}
				continue
			}

			if result == "" {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond * 100):
					// noop
				}
				continue
			}

			if !deliver(ctx, ch, &ChannelMessage{
				Message:    result,
				AckMessage: removefn,
			}) {
				return
			}
		}
	}()

	return ch
}

const (
	MaxAckIndex = 2000
	AckStep     = 50
)

// RestoreExpiredMessages will restore expired messages back to the queue.
//
// Deprecated: this is handled by the redis lua script on every call to PopMessage().
func (q Queue) RestoreExpiredMessages(ctx context.Context, limit int) {
	maxLimit := MaxAckIndex
	if limit > 0 {
		maxLimit = limit
	}

	acklistRemove := make([]string, 0, MaxAckIndex)
	ackListAdd := make([]string, 0, MaxAckIndex)

	lookupLen := q.RedisClient.LLen(ctx, q.getAckList()).Val()
	if int(lookupLen) > maxLimit {
		lookupLen = int64(maxLimit)
	}

	for i := 0; i < int(lookupLen); i++ {
		item, err := q.RedisClient.LIndex(ctx, q.getAckList(), int64(i)).Result()
		if err != nil {
			break
		}

		itsplit := strings.SplitN(item, "|", 2)
		if len(itsplit) != 2 {
			acklistRemove = append(acklistRemove, item)
		}
		timestamp, err := strconv.ParseInt(itsplit[0], 10, 64)
		if err != nil {
			acklistRemove = append(acklistRemove, item)
			continue
		}
		if time.Now().Unix() > timestamp {
			acklistRemove = append(acklistRemove, item)
			// item expired, will be added back to the queue
			ackListAdd = append(ackListAdd, itsplit[1])
		}
	}

	for i := 0; i < len(ackListAdd); i++ {
		if _, err := q.RedisClient.LPos(ctx, q.Name, ackListAdd[i], redis.LPosArgs{
			MaxLen: MaxAckIndex,
			Rank:   1,
		}).Result(); err != nil {
			if q.LeftPush {
				q.RedisClient.LPush(ctx, q.Name, ackListAdd[i])
			} else {
				q.RedisClient.RPush(ctx, q.Name, ackListAdd[i])
			}
		}
	}

	for i := 0; i < len(acklistRemove); i++ {
		q.RedisClient.LRem(ctx, q.getAckList(), 1, acklistRemove[i])
	}
}

func IsEmptyQueueError(err error) bool {
	if err == nil {
		return false
	}
	if err.Error() == "redis: nil" {
		return true
	}
	return false
}

func nonzero(v ...int) int {
	for _, i := range v {
		if i != 0 {
			return i
		}
	}
	return 0
}
