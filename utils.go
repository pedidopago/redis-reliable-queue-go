package rq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	credis "github.com/pedidopago/go-common/redis"
)

type RedisWrapper struct {
	Client *redis.Client
}

func (r RedisWrapper) Get(ctx context.Context, key string) credis.StringCmder {
	return r.Client.Get(ctx, key)
}

func (r RedisWrapper) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) credis.StatusCmder {
	return r.Client.Set(ctx, key, value, expiration)
}
