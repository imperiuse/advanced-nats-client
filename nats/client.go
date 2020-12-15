package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/imperiuse/advance-nats-client/logger"
	"github.com/imperiuse/advance-nats-client/serializable"
)

const (
	MaxReconnectDefault = 10

	ReconnectWaitDefault      = 5 * time.Second
	ReconnectJitterDefault    = time.Second * 1
	ReconnectJitterTLSDefault = time.Second * 2
)

var (
	ErrEmptyMsg = errors.New("empty msg. nats msg is nil")
)

var (
	DefaultDSN = []URL{nats.DefaultURL}
	testDSN    = []URL{"nats://127.0.0.1:4223"}
)

type SimpleNatsClientI interface {
	UseCustomLogger(logger.Logger)
	Ping(context.Context, Subj) (bool, error)
	PongHandler(Subj) (*Subscription, error)
	PongQueueHandler(Subj, QueueGroup) (*Subscription, error)
	Request(context.Context, Subj, Serializable, Serializable) error
	ReplyHandler(Subj, Serializable, Handler) (*Subscription, error)
	NatsConn() *Conn
	Close() error
}

//go:generate mockery --name=PureNatsConnI
type (
	PureNatsConnI interface {
		RequestWithContext(context.Context, Subj, []byte) (*Msg, error)
		Subscribe(Subj, MsgHandler) (*Subscription, error)
		QueueSubscribe(Subj, QueueGroup, MsgHandler) (*Subscription, error)
		Drain() error
		Close()
	}

	QueueGroup = string
	Subj       = string

	// client is Advance Nats client, or simple - wrapper for nats.Conn, so it's own nats client library for reduce code
	client struct {
		log  logger.Logger
		dsn  []URL
		conn PureNatsConnI

		pureNC *Conn // pure nats connection, for some special stuff, doesn't matter in all
	}

	URL = string // dsn url like this -> "nats://127.0.0.1:4222"

	Option       = nats.Option
	MsgHandler   = nats.MsgHandler
	Msg          = nats.Msg
	Subscription = nats.Subscription
	Conn         = nats.Conn

	//                pure NATS Msg, request   reply
	Handler      = func(*Msg, Serializable) Serializable
	Serializable = serializable.Serializable
)

const (
	pingMsg = "ping"
	pongMsg = "pong"
)

// New - main "constructor" function
func New(dsn []URL, options ...Option) (*client, error) {
	c := NewDefaultClient().addDSN(dsn)

	// Default settings for internal NATS client
	if len(options) == 0 {
		options = c.defaultNatsOptions()
	}

	conn, err := createNatsConn(dsn, options...)
	if err != nil {
		return nil, fmt.Errorf("can't create nats conn. nats unavailbale? %w", err)
	}

	c.conn = conn
	c.pureNC = conn

	return c, nil
}

func createNatsConn(dsn []URL, option ...Option) (*nats.Conn, error) {
	//	Normally, the library will return an error when trying to connect and
	//	there is no server running. The RetryOnFailedConnect option will set
	//	the connection in reconnecting state if it failed to connect right away.
	conn, err := nats.Connect(strings.Join(dsn, ", "), option...)
	if err != nil {
		// Should not return an error even if it can't connect, but you still
		// need to check in case there are some configuration errors.
		return nil, err
	}

	return conn, nil
}

func NewDefaultClient() *client {
	return &client{
		dsn: DefaultDSN,
		log: logger.Log,
	}
}

func (c *client) addDSN(dsn []URL) *client {
	if len(dsn) != 0 {
		c.dsn = dsn
	}
	return c
}

func (c *client) defaultNatsOptions() []Option {
	return []Option{
		nats.MaxReconnects(MaxReconnectDefault),
		nats.ReconnectJitter(ReconnectJitterDefault, ReconnectJitterTLSDefault),
		nats.ReconnectWait(ReconnectWaitDefault),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			c.log.Warn("Got disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.log.Warn("Got reconnected", zap.String("ConnUrl", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			c.log.Warn("Connection closed", zap.Error(nc.LastError()))
		}),
	}
}

// UseCustomLogger - register your own logger instance of zap.Logger
func (c *client) UseCustomLogger(log logger.Logger) {
	c.log = log
}

// Ping - send synchronous request with ctx deadline or timeout
// e.g. ctx, _ := context.WithTimeout(context.Background(), time.Second)
func (c *client) Ping(ctx context.Context, subj Subj) (bool, error) {
	c.log.Debug("Ping", zap.String("subj", subj))
	msg, err := c.conn.RequestWithContext(ctx, subj, []byte(pingMsg))
	if msg == nil {
		return false, ErrEmptyMsg
	}

	return string(msg.Data) == pongMsg, err
}

// PongHandler - register simple pong handler (use to peer-to-peer topics)
func (c *client) PongHandler(subj Subj) (*Subscription, error) {
	c.log.Debug("[PongHandler]", zap.String("subj", subj))
	return c.conn.Subscribe(subj, func(msg *Msg) {
		if msg == nil {
			c.log.Debug("Nil msg", zap.String("subj", subj))
			return
		}
		if string(msg.Data) == pingMsg {
			_ = msg.Respond([]byte(pongMsg))
		}
	})
}

// PongQueueHandler - register pong handler with QueueGroup (use to 1 to Many)
func (c *client) PongQueueHandler(subj Subj, queue QueueGroup) (*Subscription, error) {
	c.log.Debug("[PongQueueHandler]", zap.String("subj", subj), zap.String("queue", queue))
	return c.conn.QueueSubscribe(subj, queue, func(msg *Msg) {
		if msg == nil {
			return
		}
		if string(msg.Data) == pingMsg {
			_ = msg.Respond([]byte(pongMsg))
		}
	})
}

// Request - send synchronous msg to topic subj, and wait reply from another topic (e.g. Request-Reply Nats pattern)
func (c *client) Request(ctx context.Context, subj Subj, request Serializable, reply Serializable) error {
	c.log.Debug("[Request]", zap.String("subj", subj), zap.Any("data", request))

	byteData, err := request.Marshal()
	if err != nil {
		return err
	}

	msg, err := c.conn.RequestWithContext(ctx, subj, byteData)
	if err != nil {
		return err
	}

	if msg == nil {
		return ErrEmptyMsg
	}

	return reply.Unmarshal(msg.Data)
}

// ReplyHandler - register for asynchronous msgHandler func for process Nats Msg
func (c *client) ReplyHandler(subj Subj, awaitData Serializable, msgHandler Handler) (*Subscription, error) {
	return c.conn.Subscribe(subj, func(msg *nats.Msg) {
		if msg == nil {
			c.log.Warn("[ReplyHandler] Nil msg", zap.String("subj", subj))
			return
		}

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[ReplyHandler] Unmarshal",
				zap.String("subj", subj),
				zap.Any("msg", msg),
				zap.Error(err),
			)
			return
		}

		replyData := msgHandler(msg, awaitData)

		data, err := replyData.Marshal()
		if err != nil {
			c.log.Error("[ReplyHandler] Marshall",
				zap.String("subj", subj),
				zap.Any("data", replyData),
				zap.Error(err),
			)
			return
		}

		if err = msg.Respond(data); err != nil {
			c.log.Error("[ReplyHandler] Respond",
				zap.String("subj", subj),
				zap.Error(err),
			)
			return
		}
	})
}

// NatsConn - return pure Nats Conn (pointer to struct)
func (c *client) NatsConn() *Conn {
	return c.pureNC
}

// Close - Drain and Close workaround
func (c *client) Close() error {
	err := c.conn.Drain()
	c.conn.Close()
	return err
}
