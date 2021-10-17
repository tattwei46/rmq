package main

import (
	"context"
	"log"
	"time"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	queue := lib.New(name, addr)

	// publisher1
	go func() {
		for {
			time.Sleep(time.Second * 1)
			if err := queue.Push([]byte("message from 1")); err != nil {
				log.Printf("Push 1failed: %s\n", err)
			} else {
				log.Println("Push 1 succeeded!")
			}
		}
	}()

	// publisher2
	go func() {
		for {
			time.Sleep(time.Second * 2)
			if err := queue.Push([]byte("message from 2")); err != nil {
				log.Printf("Push 2 failed: %s\n", err)
			} else {
				log.Println("Push 2 succeeded!")
			}
		}
	}()

	// publisher3
	go func() {
		for {
			time.Sleep(time.Second * 3)
			if err := queue.Push([]byte("message from 3")); err != nil {
				log.Printf("Push 3 failed: %s\n", err)
			} else {
				log.Println("Push 3 succeeded!")
			}
		}
	}()

	queue.Consume(context.Background())

	select {}
}
