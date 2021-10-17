package lib

import "github.com/streadway/amqp"

// Stream will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
func (s *Session) Stream() (<-chan amqp.Delivery, error) {
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
