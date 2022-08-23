# redis-reliable-queue
<a href="https://github.com/pedidopago/redis-reliable-queue-go">![go](https://img.shields.io/badge/go-1.18-blue)</a>
<a href="https://github.com/pedidopago/redis-reliable-queue-js">![node-ts](https://img.shields.io/badge/node-14%2B-yellow)</a>

A Go package that implements a reliable queue that uses Redis for the backend.
It uses the RPOPLPUSH pattern:
https://redis.io/commands/rpoplpush#pattern-reliable-queue

## !Warning!

This version (2.x) is **incompatible** with publishers or subscribers of **V1.x**

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

    rq "github.com/pedidopago/redis-reliable-queue-go"
    "github.com/go-redis/redis/v8"
)

type MyData struct {
    Name string `json:"name"`
    Email string `json:"email"`
}

func main() {

    rediscl := redis.NewClient(&redis.Options{
		Addr:    os.Getenv("REDIS_ADDR"),
	})

    // instantiate
    q := rq.New(rediscl, "my_queue_name")
    // send
    q.PushMessage(context.TODO(), "hello!")

    // create a listener chan
    listenerch, closechannel := q.Listen(context.TODO(), os.Getenv("WORKER_ID"))
    defer closechannel()

    exitch := make(chan os.Signal, 1)
    signal.Notify(exitch, os.Interrupt)

    // listen until the channel is closed
    for msg := range <-listenerch {
        fmt.Println("data received:", msg.Payload)
        _ = msg.Consume() // remove this message from the processing queue
    }
}
```
