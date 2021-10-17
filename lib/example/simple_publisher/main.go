package main

import (
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	queue := lib.New(name, addr)
	message := []byte("message")
	// Attempt to push a message every 2 seconds
	for {
		time.Sleep(time.Second * 2)
		mid := uuid.New().String()
		if err := queue.Push(message, mid); err != nil {
			log.Printf("Push %s failed: %s\n", mid, err)
		} else {
			log.Printf("Push %s succeeded!\n", mid)
		}
	}
}
