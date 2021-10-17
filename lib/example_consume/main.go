package main

import (
	"log"
	"time"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	name := "job_queue"
	addr := "amqp://guest:guest@localhost:5672/"
	queue := lib.New(name, addr)
	for !queue.IsReady() {
		time.Sleep(2 * time.Second)
	}
	shovel, err := queue.Stream()
	if err != nil {
		panic(err.Error())
	}
	go func() {
		for {
			msg, ok := <-shovel
			if !ok {
				log.Fatalf("source channel closed, see the reconnect example for handling this")
			}

			if err := msg.Ack(false); err != nil {
				log.Fatalf("unable to ack msg: %v", err)
			}
			log.Println("msg ack confirmed")
		}
	}()
	done := make(chan bool)
	<-done
}
