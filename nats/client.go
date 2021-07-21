package nats

import (
	"context"
	"strings"
	"time"

	"github.com/imperiuse/advance-nats-client/logger"
	"github.com/imperiuse/advance-nats-client/serializable"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// MaxReconnectDefault -  max reconnect try cnt.
	MaxReconnectDefault = -1 // infinity

	// ReconnectWaitDefault - reconnect w8 timeout default value.
	ReconnectWaitDefault = 5 * time.Second

	// ReconnectJitterDefault -  reconnect w8 jitter timeout default value.
	ReconnectJitterDefault = time.Second * 1

	// ReconnectJitterTLSDefault -  reconnect w8 jitter TLS timeout default value.
	ReconnectJitterTLSDefault = time.Second * 2
)

// ErrEmptyMsg - empty msg. nats msg is nil.
var ErrEmptyMsg = errors.New("empty msg. nats msg is nil")

// DefaultDSN - default nats url and port.
var DefaultDSN = []URL{nats.DefaultURL}

// SimpleNatsClientI _ .
type SimpleNatsClientI interface {
	UseCustomLogger(logger.Logger)
	Ping(context.Context, Subj) (bool, error)
	PongHandler(Subj) (*Subscription, error)
	PongQueueHandler(Subj, QueueGroup) (*Subscription, error)
	Request(context.Context, Subj, Serializable, Serializable) error
	ReplyHandler(Subj, Serializable, Handler) (*Subscription, error)
	ReplyQueueHandler(Subj, QueueGroup, Serializable, Handler) (*Subscription, error)
	NatsConn() *Conn
	Close() error
}

//go:generate mockery --name=PureNatsConnI
type (
	// PureNatsConnI - pure nats conn interface.
	PureNatsConnI interface {
		RequestWithContext(ctx context.Context, subj string, data []byte) (*Msg, error)
		Subscribe(subj string, msgHandler MsgHandler) (*Subscription, error)
		QueueSubscribe(subj string, queueGroup string, msgHandler MsgHandler) (*Subscription, error)
		Drain() error
		Close()
	}

	// Subj - topic name.
	Subj string
	// QueueGroup - queue group name.
	QueueGroup string

	// client - wrapper for pure nats.Conn, so it's own nats client library for reduce code.
	client struct {
		log  logger.Logger
		dsn  []URL
		conn PureNatsConnI

		pureNC *Conn // pure nats connection, for some special stuff, doesn't matter in all
	}

	// URL - url name.
	URL = string // dsn url like this -> "nats://127.0.0.1:4222"

	// Option - nats.Msg.
	Option = nats.Option
	// MsgHandler - nats.MsgHandler.
	MsgHandler = nats.MsgHandler
	// Msg - nats.Msg.
	Msg = nats.Msg
	// Subscription - nats.Subscription.
	Subscription = nats.Subscription
	// Conn - nats.Conn struct.
	Conn = nats.Conn

	// Handler - pure NATS Msg, request   reply.
	Handler = func(*Msg, Serializable) Serializable
	// Serializable - serializable.
	Serializable = serializable.Serializable
)

const (
	pingMsg = "ping"
	pongMsg = "pong"
)

// New - main "constructor" function.
// nolint golint
func New(dsn []URL, options ...Option) (*client, error) {
	c := NewDefaultClient().addDSN(dsn)

	// Default settings for internal NATS client
	if len(options) == 0 {
		options = c.defaultNatsOptions()
	}

	conn, err := createNatsConn(dsn, options...)
	if err != nil {
		return nil, errors.Wrap(err, "can't create nats conn. nats unavailbale?")
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
		return nil, errors.Wrap(err, "create nats.Connect")
	}

	return conn, nil
}

// NewDefaultClient empty default client.
// nolint golint
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
			c.log.Warn("[DisconnectErrHandler] Disconnect", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.log.Warn("[ReconnectHandler] Reconnect", zap.String("ConnUrl", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			c.log.Warn("[ClosedHandler] Close handler", zap.Error(nc.LastError()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.log.Warn("[ErrorHandler]", zap.Error(err))
		}),
	}
}

// UseCustomLogger - register your own logger instance of zap.Logger.
func (c *client) UseCustomLogger(log logger.Logger) {
	c.log = log
}

// Ping - send synchronous request with ctx deadline or timeout
// e.g. ctx, _ := context.WithTimeout(context.Background(), time.Second).
func (c *client) Ping(ctx context.Context, subj Subj) (bool, error) {
	c.log.Debug("Ping", zap.String("subj", string(subj)))

	msg, err := c.conn.RequestWithContext(ctx, string(subj), []byte(pingMsg))
	if err != nil {
		return false, errors.Wrap(err, "c.conn.RequestWithContext")
	}

	if msg == nil {
		return false, errors.Wrap(ErrEmptyMsg, "msg == nil")
	}

	return string(msg.Data) == pongMsg, nil
}

// PongHandler - register simple pong handler (use to peer-to-peer topics).
func (c *client) PongHandler(subj Subj) (*Subscription, error) {
	c.log.Debug("[PongHandler]", zap.String("subj", string(subj)))

	return c.conn.Subscribe(string(subj), func(msg *Msg) {
		if msg == nil {
			c.log.Debug("Nil msg", zap.String("subj", string(subj)))

			return
		}
		if string(msg.Data) == pingMsg {
			_ = msg.Respond([]byte(pongMsg))
		}
	})
}

// PongQueueHandler - register pong handler with QueueGroup (use to 1 to Many).
func (c *client) PongQueueHandler(subj Subj, queue QueueGroup) (*Subscription, error) {
	c.log.Debug("[PongQueueHandler]", zap.String("subj", string(subj)), zap.String("queue", string(queue)))

	return c.conn.QueueSubscribe(string(subj), string(queue), func(msg *Msg) {
		if msg == nil {
			return
		}
		if string(msg.Data) == pingMsg {
			_ = msg.Respond([]byte(pongMsg))
		}
	})
}

// Request - send synchronous msg to topic subj, and wait reply from another topic (e.g. Request-Reply Nats pattern).
func (c *client) Request(ctx context.Context, subj Subj, request Serializable, reply Serializable) error {
	c.log.Debug("[Request]", zap.String("subj", string(subj)), zap.Any("data", request))

	byteData, err := request.Marshal()
	if err != nil {
		return errors.Wrap(err, "request.Marshal()")
	}

	msg, err := c.conn.RequestWithContext(ctx, string(subj), byteData)
	if err != nil {
		return errors.Wrap(err, "c.conn.RequestWithContext")
	}

	if msg == nil {
		return errors.Wrap(ErrEmptyMsg, "Request")
	}

	return reply.Unmarshal(msg.Data)
}

// ReplyHandler - register for asynchronous msgHandler func for process Nats Msg.
func (c *client) ReplyHandler(subj Subj, awaitData Serializable, msgHandler Handler) (*Subscription, error) {
	return c.conn.Subscribe(string(subj), func(msg *nats.Msg) {
		if msg == nil {
			c.log.Warn("[ReplyHandler] Nil msg", zap.String("subj", string(subj)))

			return
		}

		awaitData.Reset() // Important! For use clean struct

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[ReplyHandler] Unmarshal",
				zap.String("subj", string(subj)),
				zap.Any("msg", msg),
				zap.Error(err),
			)

			return
		}

		replyData := msgHandler(msg, awaitData)

		data, err := replyData.Marshal()
		if err != nil {
			c.log.Error("[ReplyHandler] Marshall",
				zap.String("subj", string(subj)),
				zap.Any("data", replyData),
				zap.Error(err),
			)

			return
		}

		if err = msg.Respond(data); err != nil {
			c.log.Error("[ReplyHandler] Respond",
				zap.String("subj", string(subj)),
				zap.Error(err),
			)

			return
		}
	})
}

// ReplyQueueHandler - register queue for asynchronous msgHandler func for process Nats Msg.
func (c *client) ReplyQueueHandler(subj Subj, qG QueueGroup, awaitData Serializable, h Handler) (*Subscription, error) {
	return c.conn.QueueSubscribe(string(subj), string(qG), func(msg *nats.Msg) {
		if msg == nil {
			c.log.Warn("[ReplyQueueHandler] Nil msg", zap.String("subj", string(subj)))

			return
		}

		awaitData.Reset() // Important! For use clean struct

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[ReplyQueueHandler] Unmarshal",
				zap.String("subj", string(subj)),
				zap.String("qGroup", string(qG)),
				zap.Any("msg", msg),
				zap.Error(err),
			)

			return
		}

		replyData := h(msg, awaitData)

		data, err := replyData.Marshal()
		if err != nil {
			c.log.Error("[ReplyQueueHandler] Marshall",
				zap.String("subj", string(subj)),
				zap.String("qGroup", string(qG)),
				zap.Any("data", replyData),
				zap.Error(err),
			)

			return
		}

		if err = msg.Respond(data); err != nil {
			c.log.Error("[ReplyQueueHandler] Respond",
				zap.String("subj", string(subj)),
				zap.String("qGroup", string(qG)),
				zap.Error(err),
			)

			return
		}
	})
}

// NatsConn - return pure Nats Conn (pointer to struct).
func (c *client) NatsConn() *Conn {
	return c.pureNC
}

// Close - Drain and Close workaround.
func (c *client) Close() error {
	defer c.conn.Close()

	if err := c.conn.Drain(); err != nil {
		return errors.Wrap(err, "c.conn.Drain")
	}

	return nil
}
