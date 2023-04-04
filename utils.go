package rq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type Client interface {
	Get(ctx context.Context, key string) StringCmder
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) StatusCmder
}

type StatusCmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	Err() error
	Val() string
	Result() (string, error)
	String() string
}

type StringCmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	Err() error
	Val() string
	Result() (string, error)
	Bytes() ([]byte, error)
	Int() (int, error)
	Int64() (int64, error)
	Uint64() (uint64, error)
	Float32() (float32, error)
	Float64() (float64, error)
	Time() (time.Time, error)
	Scan(val interface{}) error
	String() string
}

type RedisWrapper struct {
	Client *redis.Client
}

func (r RedisWrapper) Get(ctx context.Context, key string) StringCmder {
	return r.Client.Get(ctx, key)
}

func (r RedisWrapper) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) StatusCmder {
	return r.Client.Set(ctx, key, value, expiration)
}
