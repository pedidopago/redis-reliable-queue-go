package rq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

func WaitForKey(ctx context.Context, cl *redis.Client, key string) (string, error) {
	for {
		v, err := cl.Get(ctx, key).Result()
		if err == redis.Nil {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(10 * time.Millisecond):

			}
			continue
		}
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
