package main

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type channelManager struct {
	url                 string
	channel             *amqp.Channel
	connection          *amqp.Connection
	channelMux          *sync.RWMutex
	notifyCancelOrClose chan error
}

func newChannelManager(url string) (*channelManager, error) {
	conn, ch, err := newConnAndChannel(url)
	if err != nil {
		return nil, err
	}

	chManager := channelManager{
		url:                 url,
		connection:          conn,
		channel:             ch,
		channelMux:          &sync.RWMutex{},
		notifyCancelOrClose: make(chan error),
	}
	go chManager.startNotifyCancelOrClosed()
	return &chManager, nil
}

func newConnAndChannel(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url) // TODO: Explore DialConfig
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, err
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect with an exponential
// backoff. Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chManager *channelManager) startNotifyCancelOrClosed() {
	notifyCloseChan := chManager.channel.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := chManager.channel.NotifyCancel(make(chan string, 1))

	select {
	case err := <-notifyCloseChan:
		// If the connection close is triggered by the Server, a reconnection takes place
		if err != nil && err.Server {
			log.Printf("attempting to reconnect to amqp server after close")
			chManager.reconnectWithBackoff()
			log.Printf("successfully reconnected to amqp server after close")
			chManager.notifyCancelOrClose <- err
		}
	case err := <-notifyCancelChan:
		log.Printf("attempting to reconnect to amqp server after cancel")
		chManager.reconnectWithBackoff()
		log.Printf("successfully reconnected to amqp server after cancel")
		chManager.notifyCancelOrClose <- errors.New(err)
	}
}

// reconnectWithBackoff continuously attempts to reconnect with an
// exponential backoff strategy
func (chManager *channelManager) reconnectWithBackoff() {
	backoffTime := time.Second
	for {
		log.Printf("waiting %s seconds to attempt to reconnect to amqp server", backoffTime)
		time.Sleep(backoffTime)
		backoffTime *= 2
		err := chManager.reconnect()
		if err != nil {
			log.Printf("error reconnecting to amqp server: %v", err)
		} else {
			return
		}
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chManager *channelManager) reconnect() error {
	chManager.channelMux.Lock()
	defer chManager.channelMux.Unlock()
	newConn, newChannel, err := newConnAndChannel(chManager.url)
	if err != nil {
		return err
	}

	chManager.channel.Close()
	chManager.connection.Close()

	chManager.connection = newConn
	chManager.channel = newChannel
	go chManager.startNotifyCancelOrClosed()
	return nil
}

// Return captures a flattened struct of fields returned by the server when a
// Publishing is unable to be delivered either due to the `mandatory` flag set
// and no route found, or `immediate` flag set and no free consumer.
type Return struct {
	amqp.Return
}

type Confirm struct {
	ack bool
}

// Publisher allows you to publish messages safely across an open connection
type Publisher struct {
	chManager         *channelManager
	notifyReturnChan  chan Return
	confirm           bool
	notifyConfirmChan chan Confirm
}

// NewPublisher returns a new publisher with an open channel to the cluster.
// If you plan to enforce mandatory or immediate publishing, those failures will be reported
// on the channel of Returns that you should setup a listener on.
// Flow controls are automatically handled as they are sent from the server, and publishing
// will fail with an error when the server is requesting a slowdown
func NewPublisher(url string, confirm bool) (Publisher, error) {
	chManager, err := newChannelManager(url)
	if err != nil {
		return Publisher{}, err
	}

	if confirm {
		log.Print("setting channel to confirm mode")
		if err := chManager.channel.Confirm(false); err != nil {
			return Publisher{}, err
		}
	}

	publisher := Publisher{
		chManager: chManager,
		confirm:   confirm,
	}

	// restart notifiers when cancel/close is triggered
	go func() {
		for err := range publisher.chManager.notifyCancelOrClose {
			log.Printf("publish cancel/close handler triggered. err: %v", err)
			if publisher.notifyReturnChan != nil {
				go publisher.startNotifyReturnHandler()
			}
			if publisher.confirm && publisher.notifyConfirmChan != nil {
				go publisher.startNotifyConfirmHandler()
			}
		}
	}()

	return publisher, nil
}

// NotifyReturn registers a listener for basic.return methods.
// These can be sent from the server when a publish is undeliverable either from the mandatory or immediate flags.
func (publisher *Publisher) NotifyReturn() <-chan Return {
	publisher.notifyReturnChan = make(chan Return)
	go publisher.startNotifyReturnHandler()
	return publisher.notifyReturnChan
}

func (publisher *Publisher) NotifyConfirm() <-chan Confirm {
	publisher.notifyConfirmChan = make(chan Confirm)
	go publisher.startNotifyConfirmHandler()
	return publisher.notifyConfirmChan
}

// Publish publishes the provided data to the given routing keys over the connection
func (publisher *Publisher) Publish(routingKey string, data []byte) error {
	// Actual publish.
	return publisher.chManager.channel.Publish(
		"",
		routingKey,
		true,  // TODO set a option
		false, // TODO set a option
		amqp.Publishing{
			ContentType:  "application/json", // TODO set a option
			DeliveryMode: amqp.Persistent,    // TODO set a option
			Body:         data,
		},
	)
}

// StopPublishing stops the publishing of messages.
// The publisher should be discarded as it's not safe for re-use
func (publisher Publisher) StopPublishing() {
	publisher.chManager.channel.Close()
	publisher.chManager.connection.Close()
}

func (publisher *Publisher) startNotifyReturnHandler() {
	returns := publisher.chManager.channel.NotifyReturn(make(chan amqp.Return, 1))
	for ret := range returns {
		publisher.notifyReturnChan <- Return{ret}
	}
}

func (publisher *Publisher) startNotifyConfirmHandler() {
	confirms := publisher.chManager.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	for confirm := range confirms {
		publisher.notifyConfirmChan <- Confirm{confirm.Ack}
	}
}

func main() {
	const (
		url   = "amqp://guest:guest@localhost:5672/"
		queue = "else"
		body  = "test!"
	)

	p, err := NewPublisher(url, true)
	failOnError(err, "failed to create publisher")
	defer p.StopPublishing()

	err = p.Publish(queue, []byte(body))
	failOnError(err, "failed to publish a message")
	log.Printf(" [x] Sent %s", body)

	returns := p.NotifyReturn()
	go func() {
		for r := range returns {
			log.Printf("message returned from server: %s", string(r.Body))
		}
	}()

	confirms := p.NotifyConfirm()
	go func() {
		for c := range confirms {
			log.Printf("confirmation from server: %t", c.ack)
		}
	}()

	select {}
}
