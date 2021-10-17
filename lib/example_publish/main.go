package main

import (
	"fmt"
	"time"

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
		if err := queue.Push(message); err != nil {
			fmt.Printf("Push failed: %s\n", err)
		} else {
			fmt.Println("Push succeeded!")
		}
	}
}
