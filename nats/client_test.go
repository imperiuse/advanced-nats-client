package nats

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"

	"go.uber.org/zap"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/imperiuse/advance-nats-client/nats/mocks"
	m "github.com/imperiuse/advance-nats-client/serializable/mock"
)

var testDSN = []URL{"nats://127.0.0.1:4223"}

type NatsClientTestSuit struct {
	suite.Suite
	ctx           context.Context
	ctxCancel     context.CancelFunc
	natsClient    *client
	badNatsClient *client
	mockNats      *mocks.PureNatsConnI
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *NatsClientTestSuit) SetupSuite() {
	c, err := New([]URL{"nats://1.2.3.4:5678"})
	assert.Nil(suite.T(), c, "must be nil!")
	assert.NotNil(suite.T(), err, "must be error!")

	suite.natsClient, err = New(testDSN)
	assert.NotNil(suite.T(), suite.natsClient, "client must not be nil")
	assert.Nil(suite.T(), err, "err must be nil")
	assert.NotNil(suite.T(), suite.natsClient.NatsConn(), "pure nats conn must not be nil")
	suite.natsClient.UseCustomLogger(zap.NewNop())

	suite.badNatsClient, err = New(testDSN)
	suite.badNatsClient.UseCustomLogger(zap.NewNop())
	assert.NotNil(suite.T(), suite.badNatsClient, "client must not be nil")
	assert.Nil(suite.T(), err, "err must be nil")
	mockNats := &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = mockNats
	suite.mockNats = mockNats
	// Mock configurations down
	// NB: .Return(...) must return the same signature as the method being mocked.
	//mockNats.On("RequestWithContext", mock.AnythingOfType("context.Context"), mock.AnythingOfType("string"), mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil, nil) // Request(subj string, data []byte, timeout time.Duration) (*Msg, error)
	//mockNats.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, nil)                               // Subscribe(subj string, cb MsgHandler) (*Subscription, error)
	//mockNats.On("QueueSubscribe", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, nil)                               // Subscribe(subj string, cb MsgHandler) (*Subscription, error)
	//mockNats.On("Drain").Return(nil) // Drain()error
	//mockNats.On("Close").Return()    // Close()

}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *NatsClientTestSuit) TearDownSuite() {
	err := suite.natsClient.Close()
	assert.Nil(suite.T(), err)

	suite.mockNats.On("Drain").Return(nil) // Drain()error
	suite.mockNats.On("Close").Return()    // Close()

	err = suite.badNatsClient.Close()
	assert.Nil(suite.T(), err)
}

// The SetupTest method will be run before every test in the suite.
func (suite *NatsClientTestSuit) SetupTest() {
	suite.ctx, suite.ctxCancel = context.WithCancel(context.Background())
}

// The TearDownTest method will be run after every test in the suite.
func (suite *NatsClientTestSuit) TearDownTest() {
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(NatsClientTestSuit))
}

// All methods that begin with "Test" are run as tests within a
// suite.
func (suite *NatsClientTestSuit) Test_PingPong() {
	const (
		subj     = "Test_PingPong"
		timeout1 = time.Millisecond
		timeout2 = time.Second
	)
	var result bool

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc()
	result, err := suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")

	pongSubscription, err := suite.natsClient.PongHandler(subj)
	assert.Nil(suite.T(), err, "PongHandler error")
	assert.NotNil(suite.T(), pongSubscription, "pongSubscription must non nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc2()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription.Unsubscribe() err")

	ctx, cancelFunc3 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc3()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")
}

func (suite *NatsClientTestSuit) Test_PongQueueGroup() {
	const (
		subj     = "Test_PongQueueGroup"
		queue    = subj
		timeout1 = time.Millisecond
		timeout2 = time.Second
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc()
	result, err := suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")

	pongSubscription, err := suite.natsClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler error")
	assert.NotNil(suite.T(), pongSubscription, "pongSubscription must non nil")

	pongSubscription2, err := suite.natsClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler error")
	assert.NotNil(suite.T(), pongSubscription2, "pongSubscription2 must non nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc2()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription.Unsubscribe() err")

	ctx, cancelFunc3 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc3()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription2.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription2.Unsubscribe() err")

	ctx, cancelFunc4 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc4()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")
}

func (suite *NatsClientTestSuit) Test_RequestReply() {
	const (
		cntMsg             = 3
		subj               = "Test_RequestReply"
		timeoutNatsRequest = time.Millisecond * 500
		timeoutTest        = time.Millisecond * 1000
		requestData        = "mock request requestData"
		replyData          = "mock reply requestData"
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	var wg sync.WaitGroup
	sendChan := make(chan m.DataMock, cntMsg)
	wg.Add(cntMsg)
	for i := 0; i < cntMsg; i++ {
		sendChan <- m.DataMock{Data: []byte(requestData)}
	}
	close(sendChan)

	// here server side emulate send request to client
	go func(sendChan <-chan m.DataMock) {
		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.Nil(suite.T(), err, "Request err")
			assert.Equal(suite.T(), replyData, string(reply.Data))
		}
	}(sendChan)

	// here client side reply to server
	go func() {

		_, err := suite.natsClient.ReplyHandler(subj, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "example must be non nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "wrong requestData received")
			}
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		assert.Nil(suite.T(), err, "Reply handler err")
		wg.Wait()
	}()

	wg.Wait()
	time.Sleep(timeoutTest)
}

func (suite *NatsClientTestSuit) Test_RequestReplyQueue() {
	const (
		cntMsg             = 3
		subj               = "Test_RequestReplyQueue"
		qGroup             = "Test_RequestReplyQueue"
		timeoutNatsRequest = time.Millisecond * 500
		requestData        = "mock request requestData"
		replyData          = "mock reply requestData"
		timeoutTest        = time.Millisecond * 1000
	)

	var cnt atomic.Int32 // проверка что действительно только один из двух handler-ов обрабатывает request
	cnt.Store(0)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	var wg sync.WaitGroup
	sendChan := make(chan m.DataMock, cntMsg)
	wg.Add(cntMsg)
	for i := 0; i < cntMsg; i++ {
		sendChan <- m.DataMock{Data: []byte(requestData)}
	}
	close(sendChan)

	// here server side emulate send request to client
	go func(sendChan <-chan m.DataMock) {
		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.Nil(suite.T(), err, "Request err")
			assert.Equal(suite.T(), replyData, string(reply.Data))
		}
	}(sendChan)

	// here client side reply to server
	go func() {
		s, err := suite.natsClient.ReplyQueueHandler(subj, qGroup, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "example must be non nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "wrong requestData received")
			}
			cnt.Inc()
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		defer func() { assert.Nil(suite.T(), s.Unsubscribe(), "must be nil") }()
		assert.Nil(suite.T(), err, "Reply handler err")

		s2, err := suite.natsClient.ReplyQueueHandler(subj, qGroup, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "example must be non nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "wrong requestData received")
			}
			cnt.Inc()
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		defer func() { assert.Nil(suite.T(), s2.Unsubscribe(), "must be nil") }()
		assert.Nil(suite.T(), err, "Reply handler err")

		wg.Wait()
	}()

	wg.Wait()
	assert.Equal(suite.T(), cntMsg, int(cnt.Load()))
	time.Sleep(timeoutTest)
}

func (suite *NatsClientTestSuit) Test_BadRequestReply() {
	const (
		subj               = "Test_BadRequestReply"
		timeoutNatsRequest = time.Second
		cntMsg             = 1
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	done := make(chan struct{})
	sendChan := make(chan m.BadDataMock, cntMsg)
	sendChan <- m.BadDataMock{}
	close(sendChan)

	// here server side emulate send request to client
	go func(sendChan <-chan m.BadDataMock, done chan<- struct{}) {
		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.NotNil(suite.T(), err, "Must be err")
			assert.Equal(suite.T(), m.ErrBadDataMock, errors.Cause(err), "must be err errBadDataMock")
		}
		done <- struct{}{}
	}(sendChan, done)

	<-done
	close(done)
}

func (suite *NatsClientTestSuit) Test_BadSubscribe() {
	errBad := errors.New("bad")
	suite.mockNats.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, errBad) // Subscribe(subj string, cb MsgHandler) (*Subscription, error)

	s, err := suite.badNatsClient.ReplyHandler("testSubj", &m.DataMock{}, func(_ *Msg, _ Serializable) Serializable {
		return &m.DataMock{}
	})
	assert.Nil(suite.T(), s, "must be nil")
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, errors.Cause(err), "must be equals")
}

func (suite *NatsClientTestSuit) Test_BadRequest1() {
	const timeoutNatsRequest = time.Second
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	const errStrBad = "bad"
	errBad := errors.New(errStrBad)
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(&Msg{}, errBad)
	err := suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, errors.Cause(err), "must be equals")

	// it's hack, because you can't change hook with On() function =(
	suite.mockNats = &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = suite.mockNats
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil, nil)
	err = suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), ErrEmptyMsg, errors.Cause(err), "must be equals")

	// it's hack, because you can't change hook with On() function =(
	suite.mockNats = &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = suite.mockNats
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil, errBad)
	err = suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, errors.Cause(err), "must be equals")
}
