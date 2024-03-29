# redis-reliable-queue
<a href="https://github.com/pedidopago/redis-reliable-queue-go">![go](https://img.shields.io/badge/go-1.18-blue)</a>
<a href="https://github.com/pedidopago/redis-reliable-queue-js">![node-ts](https://img.shields.io/badge/node-14%2B-yellow)</a>

A Go package that implements a reliable queue that uses Redis for the backend.
It uses the RPOPLPUSH pattern:
https://redis.io/commands/rpoplpush#pattern-reliable-queue

## !Warning!

This version (3.x) is **incompatible** with publishers or subscribers of **V1.x**

References:
https://blog.tuleap.org/how-we-replaced-rabbitmq-redis

[![Go CI](https://github.com/pedidopago/redis-reliable-queue-go/actions/workflows/ci.yml/badge.svg)](https://github.com/pedidopago/redis-reliable-queue-go/actions/workflows/ci.yml)

```go
import (
    "context"
    "encoding/json"
    "fmt"
    "os"
    "os/signal"

    rq "github.com/pedidopago/redis-reliable-queue-go/v3"
    "github.com/go-redis/redis/v8"
)

func main() {

    rediscl := redis.NewClient(&redis.Options{
		Addr:    os.Getenv("REDIS_ADDR"),
	})

    ctx := context.Background()

    // instantiate
    q := rq.Queue{
        RedisClient: rediscl,
        Name: "my_queue_name",
    }

    // reschedule items after a crash
    q.RestoreExpiredMessages(ctx)

    // send
    q.PushMessage(ctx, "hello!")

    // receive
    err := q.PopMessage(ctx, func(msg string) err error {
        fmt.Println("received message:", msg)
        return nil
    })
    if err != nil && !rq.IsEmptyQueueError(err) {
        fmt.Println("[fatal] failed to process queue:", err)
        return
    }
    if rq.IsEmptyQueueError(err) {
        fmt.Println("the queue was empty!")
    }
}
```
