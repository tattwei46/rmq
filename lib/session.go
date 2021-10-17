package lib

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// Session ...
// TODO: Need to add mutex lock
type Session struct {
	queue           string
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         int32
}

// New creates a new consumer state instance, and automatically
// attempts to connect to the server.
func New(queue string, addr string) *Session {
	session := Session{
		queue: queue,
		done:  make(chan bool),
	}
	go session.handleReconnect(addr)
	return &session
}

// private
const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

func (s *Session) handleReconnect(addr string) {
	for {
		atomic.StoreInt32(&s.isReady, 0)
		log.Println("Attempting to connect")
		conn, err := s.connect(addr)
		if err != nil {
			log.Println("Failed to connect. Retrying...")
			select {
			case <-s.done:
				log.Println("User init close")
				return
			case <-time.After(reconnectDelay):

			}
			continue
		}
		if done := s.handleReInit(conn); done {
			break
		}
	}
}

func (s *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	s.changeConnection(conn)
	log.Println("Connected!")
	return conn, err
}

func (s *Session) changeConnection(conn *amqp.Connection) {
	s.connection = conn
	// create chan
	s.notifyConnClose = make(chan *amqp.Error)
	// hook chan to listen to event
	s.connection.NotifyClose(s.notifyConnClose)
}

func (s *Session) changeChannel(ch *amqp.Channel) {
	s.channel = ch
	// create chan
	s.notifyChanClose = make(chan *amqp.Error)
	s.notifyConfirm = make(chan amqp.Confirmation, 1)
	// hook chan to listen to several events
	s.channel.NotifyClose(s.notifyChanClose)
	s.channel.NotifyPublish(s.notifyConfirm)
}

func (s *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		atomic.StoreInt32(&s.isReady, 0)
		if err := s.init(conn); err != nil {
			log.Println("Failed to init channel. Retrying...")
			select {
			case <-s.done:
				log.Println("User init close")
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		select {
		case <-s.done:
			log.Println("User init close")
			return true
		case <-s.notifyConnClose:
			log.Println("Connection closed. Go back to Reconnecting...")
			return false
		case <-s.notifyChanClose:
			log.Println("Channel closed. Re-running init...")
		}
	}
}

func (s *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	// set channel to confirm mode
	if err := ch.Confirm(false); err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		s.queue,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)

	if err != nil {
		return err
	}

	s.changeChannel(ch)
	atomic.StoreInt32(&s.isReady, 1)
	log.Println("Setup completed!")
	return nil
}

func (s *Session) IsReady() bool {
	return atomic.LoadInt32(&s.isReady) == 1
}

func (s *Session) NotifyChanClosed() chan *amqp.Error {
	return s.notifyChanClose
}

func (s *Session) NotifyConnClose() chan *amqp.Error {
	return s.notifyConnClose
}
