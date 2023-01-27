// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	//go:embed queue.lua
	luaScript string
)

type Queue struct {
	RedisClient           *redis.Client
	Name                  string
	AckSuffix             string
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

func (q Queue) PopMessage(ctx context.Context, fn func(msg string) error) error {
	ackList := q.getAckList()
	popcommand := "lpop"
	if q.LeftPush {
		popcommand = "rpop"
	}
	mtimeout := time.Minute * 20
	if q.MessageExpiration != 0 {
		mtimeout = q.MessageExpiration
	}
	t1 := time.Now().Add(mtimeout).Unix()
	t1s := strconv.FormatInt(t1, 10)
	result, err := q.RedisClient.Eval(ctx, luaScript, []string{q.Name, ackList, t1s, q.getListExpiration(), popcommand, "rpush"}).Result()
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	rstring, ok := result.(string)
	if !ok {
		return fmt.Errorf("result is not a string")
	}
	ackMessage := t1s + "|" + rstring
	err = fn(rstring)
	if err != nil {
		return err
	}
	if err := q.RedisClient.LRem(ctx, ackList, 1, ackMessage).Err(); err != nil {
		fmt.Println("error removing ack message", err)
	}
	return nil
}

func (q Queue) RestoreExpiredMessages(ctx context.Context) {
	lastIndex := 0
	for ctx.Err() == nil {
		items := q.RedisClient.LRange(ctx, q.getAckList(), int64(lastIndex), int64(lastIndex)+50).Val()
		if len(items) == 0 {
			break
		}
		lastIndex += len(items)
		for _, item := range items {
			itsplit := strings.SplitN(item, "|", 2)
			if len(itsplit) != 2 {
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
			}
			timestamp, err := strconv.ParseInt(itsplit[0], 10, 64)
			if err != nil {
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
				continue
			}
			if time.Now().Unix() > timestamp {
				// item expired, will be added back to the queue
				if q.LeftPush {
					q.RedisClient.LPush(ctx, q.Name, itsplit[1])
				} else {
					q.RedisClient.RPush(ctx, q.Name, itsplit[1])
				}
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
			}
		}
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
