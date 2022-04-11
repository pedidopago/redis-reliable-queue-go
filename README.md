# redis-reliable-queue
<a href="https://github.com/pedidopago/redis-reliable-queue-go">![go](https://img.shields.io/badge/go-1.18-blue)</a>
<a href="https://github.com/pedidopago/redis-reliable-queue-js">![node-ts](https://img.shields.io/badge/js-14%2B-yellow)</a>

A Go package that implements a reliable queue that uses Redis for the backend.
It uses the RPOPLPUSH pattern:
https://redis.io/commands/rpoplpush#pattern-reliable-queue

References:
https://blog.tuleap.org/how-we-replaced-rabbitmq-redis

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
    q.PushMessage(context.TODO(), "topic", MyData{
        Name: "John Doe",
        Email: "jdoe@gmail.com"
    })

    // create a listener chan
    listenerch, close := q.Listen(context.TODO(), os.Getenv("WORKER_ID"))
    defer close()

    exitch := make(chan os.Signal, 1)
    signal.Notify(exitch, os.Interrupt)

    // listen
    for {
        select {
        case <-exitch:
            // quit
            return
        case msg := <-listenerch:
           var mymsg MyData
           _ = json.Unmarshal([]byte(msg.Content), &mymsg)
           fmt.Println("data received:", mymsg, "[topic:", msg.Topic, "]")
        }
    }
}
```
