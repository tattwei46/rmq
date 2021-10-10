package lib

import (
	"sync"

	"github.com/streadway/amqp"
)

// DeliveryMode. Transient means higher throughput but messages will not be
// restored on broker restart. The delivery mode of publishings is unrelated
// to the durability of the queues they reside on. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode. Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = amqp.Transient
	Persistent uint8 = amqp.Persistent
)

// PublishOptions are used to control how data is published
type PublishOptions struct {
	Exchange string
	// Mandatory fails to publish if there are no queues bound to routing key
	Mandatory bool
	// Immediate fails to publish if there are no consumers that can ack bound to queue on routing key
	Immediate bool
	// Transient or Persistent
	DeliveryMode uint8
}

// WithPublishOptionsExchange returns a function that sets the exchange to publish to
func WithPublishOptionsExchange(exchange string) func(*PublishOptions) {
	return func(options *PublishOptions) {
		options.Exchange = exchange
	}
}

// WithPublishOptionsMandatory makes the publishing mandatory, which means when a queue is not
// bound to the routing key a message will be sent back on the returns channel for you to handle
func WithPublishOptionsMandatory(options *PublishOptions) {
	options.Mandatory = true
}

// WithPublishOptionsImmediate makes the publishing immediate, which means when a consumer is not available
// to immediately handle the new message, a message will be sent back on the returns channel for you to handle
func WithPublishOptionsImmediate(options *PublishOptions) {
	options.Immediate = true
}

// WithPublishOptionsPersistentDelivery sets the message to persist. Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart. By default publishings
// are transient
func WithPublishOptionsPersistentDelivery(options *PublishOptions) {
	options.DeliveryMode = Persistent
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager *channelManager

	notifyReturnChan chan Return

	disablePublishDueToFlow    bool
	disablePublishDueToFlowMux *sync.RWMutex

	logger Logger
}
