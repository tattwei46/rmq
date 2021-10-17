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
	pubQueue := lib.New(name, addr)
	conQueue := lib.New(name, addr)

	//publisher1
	go func() {
		for {
			time.Sleep(time.Second * 1)
			mID := uuid.New().String()
			if err := pubQueue.Push([]byte("message from 1"), mID); err != nil {
				log.Printf("Push 1 %s failed: %s\n", mID, err)
			} else {
				log.Printf("Push 1 %s succeeded!\n", mID)
			}
		}
	}()

	// publisher2
	go func() {
		for {
			time.Sleep(time.Second * 2)
			mID := uuid.New().String()
			if err := pubQueue.Push([]byte("message from 2"), mID); err != nil {
				log.Printf("Push 2 %s failed: %s\n", mID, err)
			} else {
				log.Printf("Push 2 %s succeeded!\n", mID)
			}
		}
	}()

	// publisher3
	go func() {
		for {
			time.Sleep(time.Second * 3)
			mID := uuid.New().String()
			if err := pubQueue.Push([]byte("message from 3"), mID); err != nil {
				log.Printf("Push 3 %s failed: %s\n", mID, err)
			} else {
				log.Printf("Push 3 %s succeeded!\n", mID)
			}
		}
	}()

	conQueue.Consume(context.Background())

	select {}
}
