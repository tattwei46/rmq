package main

import (
	"log"
	"time"

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
			if err := queue.Push(message); err != nil {
				log.Printf("Push 1 failed: %s\n", err)
			} else {
				log.Println("Push 1 succeeded!")
			}
		}
	}()

	// goroutine2
	go func() {
		for {
			time.Sleep(time.Second * 2)
			if err := queue.Push(message); err != nil {
				log.Printf("Push 2 failed: %s\n", err)
			} else {
				log.Println("Push 2 succeeded!")
			}
		}
	}()

	// goroutine3
	go func() {
		for {
			time.Sleep(time.Second * 3)
			if err := queue.Push(message); err != nil {
				log.Printf("Push 3 failed: %s\n", err)
			} else {
				log.Println("Push 3 succeeded!")
			}
		}
	}()

	// try to kill amqp server halfway and observer all goroutines with shared queue retry connection

	// blocking statement
	select {}
}
