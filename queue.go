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

const (
	MaxAckIndex = 2000
	AckStep     = 50
)

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
