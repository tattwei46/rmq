package main

import (
	"log"
	"time"

	"github.com/tattwei46/rmq/lib"
)

func main() {
	go func() {
		for {
			name := "job_queue"
			addr := "amqp://guest:guest@localhost:5672/"
			queue := lib.New(name, addr)

			for !queue.IsReady() {
				log.Println("consumer is not ready. Try again later...")
				time.Sleep(2 * time.Second)
			}

			delivery, err := queue.Stream()
			if err != nil {
				log.Printf("error when stream setup: %v", err)
				continue
			}
			log.Println("Stream successfully setup")

			for {
				var connDrop bool
				select {
				case msg, ok := <-delivery:
					if !ok {
						log.Println("Source channel closed. Attempting to redo stream setup")
						connDrop = true
						break
					}
					if err := msg.Ack(false); err != nil {
						log.Printf("unable to ack msg: %v", err)
						continue
					}
					log.Println("msg ack confirmed")

				case <-queue.NotifyConnClose():
				case <-queue.NotifyChanClosed():
					log.Println("Source conn/chanel closed. Attempting to redo stream setup")
					connDrop = true
					break
				}
				if connDrop {
					break
				}
			}
		}
	}()
	done := make(chan bool)
	<-done
}
