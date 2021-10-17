package lib

import (
	"context"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Delivery will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (s *Session) Delivery() (<-chan amqp.Delivery, error) {
	if !s.IsReady() {
		return nil, errNotConnected
	}
	return s.channel.Consume(
		s.queue,
		"",    // Consumer
		false, // Auto-Ack
		false, // Exclusive
		false, // No-local
		false, // No-Wait
		nil,   // Args
	)
}

func (s *Session) Consume(ctx context.Context) {
	go func() {
		for {
			// initialization will take some time
			for !s.IsReady() {
				log.Println("Consumer is not ready. Try again later...")
				time.Sleep(reInitDelay)
			}

			// listen to any context cancellation
			select {
			case <-ctx.Done():
				log.Println("Context cancelled. Ending Consume.")
				break
			default:
			}

			// retrieve delivery channel
			delivery, err := s.Delivery()
			if err != nil {
				log.Printf("error when stream setup: %v\n", err)
				continue
			}

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
						log.Printf("unable to ack msg: %v\n", err)
						continue
					}
					log.Println("msg ack confirmed")

				case <-s.NotifyConnClose():
				case <-s.NotifyChanClosed():
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
}
