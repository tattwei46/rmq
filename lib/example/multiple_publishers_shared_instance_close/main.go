package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	// this single queue will be shared by goroutine1 and goroutine2
	queue := lib.New(name, addr)
	message := []byte("message")

	// goroutine1
	go func() {
		for {
			time.Sleep(time.Second * 1)
			if err := queue.Push(message, uuid.New().String()); err != nil {
				log.Printf("Push 1 failed: %s\n", err)
			} else {
				log.Println("Push 1 succeeded!")
			}
		}
	}()

	// goroutine2 - when it is done, it will close the shared queue. The goroutine1 will not retry connection.
	cancellable, _ := context.WithTimeout(context.Background(), 3*time.Second)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				log.Println("Go routine 2 will close its connection")
				if err := queue.Close(); err != nil {
					log.Printf("Received error when closing queue: %v", err)
				}
				return
			default:
				time.Sleep(time.Second * 2)
				if err := queue.Push(message, uuid.New().String()); err != nil {
					log.Printf("Push 2 failed: %s\n", err)
				} else {
					log.Println("Push 2 succeeded!")
				}
			}
		}
	}(cancellable)

	// blocking statement
	select {}
}
