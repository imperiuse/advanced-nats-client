package streaming

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/imperiuse/advance-nats-client/logger"
	nc "github.com/imperiuse/advance-nats-client/nats"
	"github.com/imperiuse/advance-nats-client/serializable"
	"github.com/imperiuse/advance-nats-client/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// AdvanceNatsClient - advance nats / nats streaming client.
type AdvanceNatsClient interface {
	// NATS @see -> nc.SimpleNatsClientI
	Ping(context.Context, nc.Subj) (bool, error)
	PongHandler(nc.Subj) (*nc.Subscription, error)
	PongQueueHandler(nc.Subj, nc.QueueGroup) (*nc.Subscription, error)
	Request(context.Context, Subj, Serializable, Serializable) error
	ReplyHandler(Subj, Serializable, nc.Handler) (*nc.Subscription, error)
	ReplyQueueHandler(Subj, QueueGroup, Serializable, nc.Handler) (*nc.Subscription, error)

	// NATS Streaming
	PublishSync(Subj, Serializable) error
	PublishAsync(Subj, Serializable, AckHandler) (GUID, error)
	DefaultAckHandler() AckHandler
	Subscribe(Subj, Serializable, Handler, ...SubscriptionOption) (Subscription, error)
	QueueSubscribe(Subj, QueueGroup, Serializable, Handler, ...SubscriptionOption) (Subscription, error)
	Reconnect(bool) error
	RegisterAfterReconnectCallbackChan(chan interface{})
	DeregisterAfterReconnectCallbackChan()

	// General for both NATS and NATS Streaming
	UseCustomLogger(logger.Logger)
	NatsConn() *nats.Conn
	Nats() nc.SimpleNatsClientI
	Close() error
}

type (
	client struct {
		clusterID string
		clientID  string
		options   []Option

		log logger.Logger
		nc  nc.SimpleNatsClientI // Simple Nats client (from another package of this library =) )

		m            sync.RWMutex
		sc           PureNatsStunConnI // StunConnI equals stan.Conn
		callbackChan chan interface{}
	}

	// URL - url.
	URL = string

	// Option - option.
	Option = stan.Option
)

//nolint golint
//go:generate mockery --name=PureNatsStunConnI
type (
	// StunConnI represents a connection to the NATS Streaming subsystem. It can Publish and
	// Subscribe to messages within the NATS Streaming cluster.
	// The connection is safe to use in multiple Go routines concurrently.
	PureNatsStunConnI interface {
		Publish(subj string, data []byte) error
		PublishAsync(subj string, data []byte, ackHandler AckHandler) (string, error)
		Subscribe(subj string, msgHandler MsgHandler, subscriptionOptions ...SubscriptionOption) (Subscription, error)
		QueueSubscribe(subj string, queueGroup string, msgHandler MsgHandler, subscriptionOptions ...SubscriptionOption) (Subscription, error)
		Close() error
	}

	Handler = func(*stan.Msg, Serializable)

	Subj       = nc.Subj
	QueueGroup = nc.QueueGroup

	Msg                = stan.Msg
	MsgHandler         = stan.MsgHandler // func(msg *Msg)
	Subscription       = stan.Subscription
	SubscriptionOption = stan.SubscriptionOption
	AckHandler         = stan.AckHandler // func(string, error)

	GUID = string // id send msg from Nats Streaming

	Serializable = serializable.Serializable

	AfterReconnectFunc = func(anc AdvanceNatsClient)
)

// EmptyGUID  "".
const EmptyGUID = ""

// nolint golint
var (
	DefaultClusterID  = "test-cluster"
	EmptyHandler      = func(*Msg, Serializable) {}
	DurableNameOption = stan.DurableName
)

var (
	// ErrNilNatsConn pure nats is nil, check usage library.
	ErrNilNatsConn = errors.New("pure nats connection from advance nats client is nil, nc.NatsConn() == nil ")

	// ErrNilNatsClient pure nats client is nil, check usage library.
	ErrNilNatsClient = errors.New("advance nats client is nil")
)

// New - Create Nats Streaming client with instance of Advance Nats client.
//nolint golint
func New(clusterID string, clientID string, nc nc.SimpleNatsClientI, options ...Option) (*client, error) {
	if nc != nil {
		if nc.NatsConn() == nil {
			return nil, errors.Wrap(ErrNilNatsConn, "[New]")
		}

		options = append(options, stan.NatsConn(nc.NatsConn()))
	}

	c, err := NewOnlyStreaming(clusterID, clientID, nil, options...)
	if err != nil || c == nil {
		return nil, errors.Wrap(err, "[New] NewOnlyStreaming")
	}

	c.nc = nc

	return c, nil
}

// NewOnlyStreaming - create only streaming client.
// nolint golint
func NewOnlyStreaming(clusterID string, clientID string, dsn []URL, options ...Option) (*client, error) {
	c := NewDefaultClient()
	c.clusterID = clusterID

	if clientID == "" {
		c.clientID = uuid.UUID4()
	} else {
		c.clientID = clientID
	}

	if options == nil {
		// Default settings for internal NATS client
		options = c.DefaultNatsStreamingOptions()
	}

	// DSN for NATS connection, e.g. "nats://127.0.0.1:4222" stan.DefaultNatsURL
	if dsn != nil {
		options = append(options, stan.NatsURL(strings.Join(dsn, ", ")))
	}

	c.options = options

	sc, err := stan.Connect(c.clusterID, c.clientID, c.options...)
	if err != nil {
		return nil, errors.Wrap(err, "[NewOnlyStreaming] can't create nats-streaming conn")
	}

	c.m.Lock()
	defer c.m.Unlock()

	c.sc = sc

	return c, nil
}

// NewDefaultClient  - NewDefaultClient.
//nolint
func NewDefaultClient() *client {
	return &client{
		log:          logger.Log,
		m:            sync.RWMutex{},
		callbackChan: nil,
	}
}

func (c *client) DefaultNatsStreamingOptions() []Option {
	const (
		maxTry           = 5
		timeoutReconnect = time.Second
	)

	return []Option{
		stan.Pings(stan.DefaultPingInterval, stan.DefaultPingMaxOut), // todo, maybe should increase, very hard
		stan.ConnectWait(stan.DefaultConnectWait),
		stan.MaxPubAcksInflight(stan.DefaultMaxPubAcksInflight),
		stan.PubAckWait(stan.DefaultAckWait),
		stan.SetConnectionLostHandler(func(sc stan.Conn, reason error) {
			for i := 0; i < maxTry; i++ {
				c.log.Sugar().Infof("[ConnectionLostHandler] Try recreate stan conn: %d", i)

				err := c.Reconnect(false)
				if err == nil {
					c.log.Sugar().Infof("[ConnectionLostHandler] Reconnect successfully: %d", i)

					return
				}

				c.log.Sugar().Warn("[ConnectionLostHandler] Reconnect %d failed: %v", i, err)

				time.Sleep(timeoutReconnect)
			}

			c.log.Warn("[ConnectionLostHandler] Reconnect attempts finished.... :")
		}),
	}
}

// Wrapper for Nats Simple Client

// Ping - under the hood wrapper for nc.Ping.
func (c *client) Ping(ctx context.Context, subj nc.Subj) (bool, error) {
	if c.nc == nil {
		return false, errors.Wrap(ErrNilNatsClient, "[Ping]")
	}

	return c.nc.Ping(ctx, subj)
}

// PongHandler - under the hood wrapper for nc.PongHandler.
func (c *client) PongHandler(subj nc.Subj) (*nc.Subscription, error) {
	if c.nc == nil {
		return nil, errors.Wrap(ErrNilNatsClient, "[PongHandler]")
	}

	return c.nc.PongHandler(subj)
}

// PongQueueHandler - under the hood wrapper for nc.PongQueueHandler.
func (c *client) PongQueueHandler(subj nc.Subj, qgroup nc.QueueGroup) (*nc.Subscription, error) {
	if c.nc == nil {
		return nil, errors.Wrap(ErrNilNatsClient, "[PongQueueHandler]")
	}

	return c.nc.PongQueueHandler(subj, qgroup)
}

// Request under the hood used simple NATS connect and simple Request-Reply semantic with at most once guarantee.
func (c *client) Request(ctx context.Context, subj Subj, requestData Serializable, replyData Serializable) error {
	if c.nc == nil {
		return errors.Wrap(ErrNilNatsClient, "[Request]")
	}

	return c.nc.Request(ctx, subj, requestData, replyData)
}

// ReplyHandler under the hood used simple Advance NATS client, Reply semantic with at most once.
func (c *client) ReplyHandler(subj Subj, awaitData Serializable, msgHandler nc.Handler) (*nc.Subscription, error) {
	if c.nc == nil {
		return nil, errors.Wrap(ErrNilNatsClient, "[ReplyHandler]")
	}

	return c.nc.ReplyHandler(subj, awaitData, msgHandler)
}

// ReplyQueueHandler under the hood used simple Advance NATS client, Reply semantic with at most once.
func (c *client) ReplyQueueHandler(subj Subj, qG QueueGroup, awD Serializable, h nc.Handler) (*nc.Subscription, error) {
	if c.nc == nil {
		return nil, errors.Wrap(ErrNilNatsClient, "[ReplyQueueHandler]")
	}

	return c.nc.ReplyQueueHandler(subj, qG, awD, h)
}

func (c *client) UseCustomLogger(log logger.Logger) {
	c.log = log
	if c.nc != nil {
		c.nc.UseCustomLogger(c.log)
	}
}

// PublishSync will publish to the NATS Streaming cluster and wait for an ACK.
func (c *client) PublishSync(subj Subj, data Serializable) error {
	c.log.Debug("[PublishSync]",
		zap.String("subj", string(subj)),
	)

	b, err := data.Marshal()
	if err != nil {
		c.log.Error("[PublishSync] Marshall",
			zap.String("subj", string(subj)),
			zap.Error(err),
		)

		return errors.Wrap(err, "[PublishSync]")
	}

	c.m.RLock()
	err = c.sc.Publish(string(subj), b)
	c.m.RUnlock()

	if errors.Is(err, stan.ErrConnectionClosed) {
		return c.Reconnect(false)
	}

	return err
}

// PublishAsync will publish to the cluster and asynchronously process
// the ACK or error state. It will return the GUID for the message being sent.
func (c *client) PublishAsync(subj Subj, data Serializable, ah AckHandler) (GUID, error) {
	c.log.Debug("[PublishAsync]",
		zap.String("subj", string(subj)),
		zap.Any("data", data),
	)

	b, err := data.Marshal()
	if err != nil {
		c.log.Error("[PublishAsync] Marshall",
			zap.String("subj", string(subj)),
			zap.Error(err))

		return EmptyGUID, errors.Wrap(err, "[PublishAsync]")
	}

	if ah == nil {
		c.log.Debug("[PublishAsync] AckHandler does not set. Use DefaultAckHandler",
			zap.String("subj", string(subj)),
		)

		ah = c.DefaultAckHandler()
	}

	c.m.RLock()
	guid, err := c.sc.PublishAsync(string(subj), b, ah)
	c.m.RUnlock()

	if errors.Is(err, stan.ErrConnectionClosed) {
		if err = c.Reconnect(false); err != nil {
		}
	}

	return guid, err
}

// DefaultAckHandler - return default ack func with logging problem's, !please better use own ack handler func!
func (c *client) DefaultAckHandler() AckHandler {
	return func(ackUID string, err error) {
		if err != nil {
			c.log.Error("Warning: error publishing msg", zap.Error(err), zap.String("msg_id", ackUID))
		} else {
			c.log.Debug("Received ack for msg", zap.String("msg_id", ackUID))
		}
	}
}

// Subscribe - func for subscribe on any subject, if no options received - default for all options append stan.SetManualAckMode().
//nolint lll
func (c *client) Subscribe(subj Subj, awaitData Serializable, handler Handler, opt ...SubscriptionOption) (Subscription, error) {
	// NB! stan.StartWithLastReceived()) - начинает с последнего уже доставленного! с виду кажется дубль
	opt = append(opt, stan.SetManualAckMode())

	msgHandler := func(msg *Msg) {
		if err := msg.Ack(); err != nil { // manual fast ack
			c.log.Error("[Subscribe] msg.Ack() problem",
				zap.Any("msg", msg),
				zap.String("subj", string(subj)),
				zap.Error(err))

			return
		}

		if msg.Redelivered {
			c.log.Warn("[Subscribe] Redelivered msg received",
				zap.Any("msg", msg),
				zap.String("subj", string(subj)))

			return // TODO. THINK HERE. WHAT WE NEED TO DO?
		}

		if msg == nil {
			c.log.Warn("[Subscribe] Msg is nil",
				zap.Any("msg", msg),
				zap.String("subj", string(subj)))

			return
		}

		awaitData.Reset() // Important! For use clean struct

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[Subscribe] Unmarshal",
				zap.Error(err),
				zap.Any("msg", msg),
				zap.String("subj", string(subj)))

			return
		}

		handler(msg, awaitData)
	}

	c.log.Debug("[Subscribe]", zap.String("subj", string(subj)))

	c.m.RLock()
	defer c.m.RUnlock()
	return c.sc.Subscribe(string(subj), msgHandler, opt...)
}

// QueueSubscribe will perform a queue subscription with the given options to the cluster.
// If no option is specified, DefaultSubscriptionOptions are used. The default start
// position is to receive new messages only (messages published after the subscription is
// registered in the cluster).
// nolint lll
func (c *client) QueueSubscribe(subj Subj, qG QueueGroup, awaitData Serializable, h Handler, opt ...SubscriptionOption) (Subscription, error) {
	// NB! stan.StartWithLastReceived()) - начинает с последнего уже доставленного! с виду кажется дубль
	opt = append(opt, stan.SetManualAckMode())

	msgHandler := func(msg *Msg) {
		if err := msg.Ack(); err != nil { // manual fast ack
			c.log.Error("[QueueSubscribe] msg.Ack() problem",
				zap.String("subj", string(subj)),
				zap.String("qgroup", string(qG)),
				zap.Any("msg", msg),
				zap.Error(err))

			return
		}

		if msg.Redelivered {
			c.log.Warn("[QueueSubscribe] Redelivered msg received",
				zap.Any("msg", msg),
				zap.String("subj", string(subj)),
				zap.String("qgroup", string(qG)))

			return // TODO. THINK HERE. WHAT WE NEED TO DO?
		}

		if msg == nil {
			c.log.Warn("[QueueSubscribe] Msg is nil",
				zap.String("subj", string(subj)),
				zap.String("qgroup", string(qG)))

			return
		}

		awaitData.Reset() // Important! For use clean struct

		if err := awaitData.Unmarshal(msg.Data); err != nil {
			c.log.Error("[QueueSubscribe] Unmarshal",
				zap.String("subj", string(subj)),
				zap.String("qgroup", string(qG)),
				zap.Any("msg", msg),
				zap.Error(err))

			return
		}

		h(msg, awaitData)
	}

	c.log.Debug("[QueueSubscribe]", zap.String("subj", string(subj)), zap.String("qgroup", string(qG)))

	c.m.RLock()
	defer c.m.RUnlock()
	return c.sc.QueueSubscribe(string(subj), string(qG), msgHandler, opt...)
}

// Nats - return advance Nats client.
func (c *client) Nats() nc.SimpleNatsClientI {
	return c.nc
}

// NatsConn - return pure nats conn pointer.
func (c *client) NatsConn() *nats.Conn {
	if c.nc == nil {
		return nil
	}

	return c.nc.NatsConn()
}

func (c *client) Reconnect(withNewClientID bool) error {
	c.m.Lock()
	defer c.m.Unlock()

	if !withNewClientID {
		c.clientID = uuid.UUID4()
	}

	sc, err := stan.Connect(c.clusterID, c.clientID, c.options...)
	if err != nil {
		return errors.Wrap(err, "[Reconnect] can't create nats streaming connection. stan.Connect - error")
	}

	c.sc = sc

	if c.callbackChan != nil {
		select {
		case c.callbackChan <- new(interface{}):
		default:
			c.log.Warn("[Reconnect] full callbackChan")
		}
	}

	return nil
}

func (c *client) RegisterAfterReconnectCallbackChan(ch chan interface{}) {
	c.m.Lock()
	defer c.m.Unlock()

	c.callbackChan = ch
}

func (c *client) DeregisterAfterReconnectCallbackChan() {
	c.m.Lock()
	defer c.m.Unlock()

	c.callbackChan = nil
}

// Close - close Nats streaming connection and NB! Also Close pure Nats Connection.
// !Note that you will be responsible for closing the NATS Connection after the streaming connection has been closed.
func (c *client) Close() error {
	var err error

	defer func() {
		if c.nc != nil {
			err1 := c.nc.Close()

			if err == nil {
				err = err1
			} else {
				err = errors.Wrap(err, fmt.Sprint(err1))
			}
		}
	}()

	c.m.RLock()
	defer c.m.RUnlock()

	if c.sc != nil {
		err = c.sc.Close()

		if err != nil {
			return errors.Wrap(err, "Close stun conn")
		}
	}

	return nil
}
