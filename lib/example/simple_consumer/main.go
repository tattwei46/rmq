package main

import (
	"context"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	queue := lib.New(name, addr)
	ctx := context.Background()

	queue.Consume(ctx)
	// blocking channel
	done := make(chan bool)
	<-done
}
