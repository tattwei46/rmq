package lib

import (
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Push will push data onto the queue, and wait for a confirm.
// If no confirms are received until within the resendTimeout,
// it continuously re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (s *Session) Push(data []byte) error {
	if !s.isReady {
		return errors.New("failed to push: not connected")
	}

	for {
		err := s.UnsafePush(data)
		if err != nil {
			log.Println("Push failed. Retrying...")
			select {
			case <-s.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-s.notifyConfirm:
			if confirm.Ack {
				log.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		log.Println("Push didn't confirm. Retrying...")
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (s *Session) UnsafePush(data []byte) error {
	if !s.isReady {
		return errNotConnected
	}
	return s.channel.Publish(
		"",      // Exchange
		s.queue, // Routing key
		false,   // Mandatory
		false,   // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}
