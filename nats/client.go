package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/imperiuse/advanced-nats-client/v1/logger"
	"github.com/imperiuse/advanced-nats-client/v1/serializable"
)

const (
	// MaxReconnectDefault - max number of reconnect attempts.
	MaxReconnectDefault = -1 // infinite

	// ReconnectWaitDefault - default reconnect wait timeout value.
	ReconnectWaitDefault = 5 * time.Second

	// ReconnectJitterDefault - default reconnect jitter wait timeout value.
	ReconnectJitterDefault = 1 * time.Second

	// ReconnectJitterTLSDefault - default reconnect jitter TLS wait timeout value.
	ReconnectJitterTLSDefault = 2 * time.Second
)

// ErrEmptyMsg - error returned when the NATS message is nil.
var ErrEmptyMsg = errors.New("empty message: NATS message is nil")

// DefaultDSN - default NATS URL and port.
var DefaultDSN = []URL{nats.DefaultURL}

// SimpleNatsClientI - interface defining basic operations of a NATS client.
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
	// PureNatsConnI - interface for pure NATS connection.
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

	// Client - wrapper around the pure nats.Conn, a custom NATS client to reduce repetitive code.
	client struct {
		log  logger.Logger
		dsn  []URL //nolint:unused
		conn PureNatsConnI

		pureNC *Conn // pure NATS connection, used for special operations, generally not required
	}

	// URL - URL string.
	URL = string // DSN URL, e.g., "nats://127.0.0.1:4222"

	// Option - alias for nats.Option.
	Option = nats.Option
	// MsgHandler - alias for nats.MsgHandler.
	MsgHandler = nats.MsgHandler
	// Msg - alias for nats.Msg.
	Msg = nats.Msg
	// Subscription - alias for nats.Subscription.
	Subscription = nats.Subscription
	// Conn - alias for nats.Conn.
	Conn = nats.Conn

	// Handler - handler function to process pure NATS messages and reply.
	Handler = func(*Msg, Serializable) Serializable
	// Serializable - serializable interface.
	Serializable = serializable.Serializable
)

const (
	pingMsg = "ping"
	pongMsg = "pong"
)

// New - main constructor function.
// nolint: golint
func New(dsn []URL, options ...Option) (*client, error) {
	c := NewDefaultClient().addDSN(dsn)

	// Default settings for the internal NATS client
	if len(options) == 0 {
		options = c.defaultNatsOptions()
	}

	conn, err := createNatsConn(dsn, options...)
	if err != nil {
		return nil, fmt.Errorf("unable to create NATS connection: is NATS unavailable?: %w", err)
	}

	c.conn = conn
	c.pureNC = conn

	return c, nil
}

func createNatsConn(dsn []URL, options ...Option) (*nats.Conn, error) {
	// Normally, the library returns an error when attempting to connect if there is no server running.
	// The RetryOnFailedConnect option sets the connection to a reconnecting state if the initial connection attempt fails.
	conn, err := nats.Connect(strings.Join(dsn, ", "), options...)
	if err != nil {
		// Should not return an error even if it can't connect immediately, but still check for configuration errors.
		return nil, fmt.Errorf("error creating nats.Connect: %w", err)
	}

	return conn, nil
}

// NewDefaultClient creates an empty default client.
// nolint: golint
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
			c.log.Warn("[DisconnectErrHandler] Disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			c.log.Warn("[ReconnectHandler] Reconnected", zap.String("ConnUrl", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			c.log.Warn("[ClosedHandler] Connection closed", zap.Error(nc.LastError()))
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			c.log.Warn("[ErrorHandler] NATS error", zap.Error(err))
		}),
	}
}

// UseCustomLogger registers a custom logger instance.
func (c *client) UseCustomLogger(log logger.Logger) {
	c.log = log
}

// Ping sends a synchronous request with a context deadline or timeout.
func (c *client) Ping(ctx context.Context, subj Subj) (bool, error) {
	c.log.Debug("Ping", zap.String("subj", string(subj)))

	msg, err := c.conn.RequestWithContext(ctx, string(subj), []byte(pingMsg))
	if err != nil {
		return false, fmt.Errorf("c.conn.RequestWithContext: %w", err)
	}

	if msg == nil {
		return false, fmt.Errorf("msg is nil: %w", ErrEmptyMsg)
	}

	return string(msg.Data) == pongMsg, nil
}

// PongHandler registers a simple pong handler (used for peer-to-peer topics).
func (c *client) PongHandler(subj Subj) (*Subscription, error) {
	c.log.Debug("[PongHandler]", zap.String("subj", string(subj)))

	return c.conn.Subscribe(string(subj), func(msg *Msg) {
		if msg == nil {
			c.log.Debug("Received nil msg", zap.String("subj", string(subj)))

			return
		}

		if string(msg.Data) == pingMsg {
			_ = msg.Respond([]byte(pongMsg))
		}
	})
}

// PongQueueHandler registers a pong handler with QueueGroup (used for 1-to-many communication).
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

// Request sends a synchronous message to a subject and waits for a reply (following the Request-Reply NATS pattern).
func (c *client) Request(ctx context.Context, subj Subj, request, reply Serializable) error {
	c.log.Debug("[Request]", zap.String("subj", string(subj)), zap.Any("data", request))

	byteData, err := request.Marshal()
	if err != nil {
		return fmt.Errorf("request.Marshal(): %w", err)
	}

	msg, err := c.conn.RequestWithContext(ctx, string(subj), byteData)
	if err != nil {
		return fmt.Errorf("c.conn.RequestWithContext: %w", err)
	}

	if msg == nil {
		return fmt.Errorf("*client.Request(): %w", ErrEmptyMsg)
	}

	return reply.Unmarshal(msg.Data)
}

// ReplyHandler registers an asynchronous message handler for processing NATS messages.
func (c *client) ReplyHandler(subj Subj, awaitData Serializable, msgHandler Handler) (*Subscription, error) {
	return c.conn.Subscribe(string(subj), func(msg *nats.Msg) {
		if msg == nil {
			c.log.Warn("[ReplyHandler] Received nil message", zap.String("subj", string(subj)))

			return
		}

		awaitData.Reset() // Important! Clean struct before use.

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[ReplyHandler] Unmarshal failed",
				zap.String("subj", string(subj)),
				zap.Any("msg", msg),
				zap.Error(err),
			)

			return
		}

		replyData := msgHandler(msg, awaitData)

		data, err := replyData.Marshal()
		if err != nil {
			c.log.Error("[ReplyHandler] Marshal failed",
				zap.String("subj", string(subj)),
				zap.Any("data", replyData),
				zap.Error(err),
			)

			return
		}

		if err = msg.Respond(data); err != nil {
			c.log.Error("[ReplyHandler] Response failed",
				zap.String("subj", string(subj)),
				zap.Error(err),
			)
		}
	})
}

// ReplyQueueHandler registers a queue for asynchronous message processing with a handler.
func (c *client) ReplyQueueHandler(subj Subj, qG QueueGroup, awaitData Serializable, h Handler) (*Subscription, error) {
	return c.conn.QueueSubscribe(string(subj), string(qG), func(msg *nats.Msg) {
		if msg == nil {
			c.log.Warn("[ReplyQueueHandler] Received nil message", zap.String("subj", string(subj)))

			return
		}

		awaitData.Reset() // Important! Clean struct before use.

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[ReplyQueueHandler] Unmarshal failed",
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
			c.log.Error("[ReplyQueueHandler] Marshal failed",
				zap.String("subj", string(subj)),
				zap.String("qGroup", string(qG)),
				zap.Any("data", replyData),
				zap.Error(err),
			)

			return
		}

		if err = msg.Respond(data); err != nil {
			c.log.Error("[ReplyQueueHandler] Response failed",
				zap.String("subj", string(subj)),
				zap.String("qGroup", string(qG)),
				zap.Error(err),
			)
		}
	})
}

// NatsConn returns the pure NATS connection (pointer to the connection struct).
func (c *client) NatsConn() *Conn {
	return c.pureNC
}

// Close drains and closes the NATS connection.
func (c *client) Close() error {
	defer c.conn.Close()

	if err := c.conn.Drain(); err != nil {
		return fmt.Errorf("c.conn.Drain: %w", err)
	}

	return nil
}
