package nats

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/imperiuse/advanced-nats-client/v1/nats/mocks"
	m "github.com/imperiuse/advanced-nats-client/v1/serializable/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var testDSN = []URL{"nats://127.0.0.1:4223"}

type NatsClientTestSuite struct {
	suite.Suite
	ctx           context.Context
	ctxCancel     context.CancelFunc
	natsClient    *client
	badNatsClient *client
	mockNats      *mocks.PureNatsConnI
}

// SetupSuite will be run by testify once at the very start of the test suite before any tests are executed.
func (suite *NatsClientTestSuite) SetupSuite() {
	c, err := New([]URL{"nats://1.2.3.4:5678"})
	assert.Nil(suite.T(), c, "client must be nil!")
	assert.NotNil(suite.T(), err, "must return an error!")

	suite.natsClient, err = New(testDSN)
	assert.NotNil(suite.T(), suite.natsClient, "client must not be nil")
	assert.Nil(suite.T(), err, "error must be nil")
	assert.NotNil(suite.T(), suite.natsClient.NatsConn(), "NATS connection must not be nil")
	suite.natsClient.UseCustomLogger(zap.NewNop())

	suite.badNatsClient, err = New(testDSN)
	suite.badNatsClient.UseCustomLogger(zap.NewNop())
	assert.NotNil(suite.T(), suite.badNatsClient, "client must not be nil")
	assert.Nil(suite.T(), err, "error must be nil")
	mockNats := &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = mockNats
	suite.mockNats = mockNats

	// Mock configurations
	// NB: .Return(...) must return the same signature as the method being mocked.
	//mockNats.On("RequestWithContext", mock.AnythingOfType("context.Context"), mock.AnythingOfType("string"), mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil, nil) // Request(subj string, data []byte, timeout time.Duration) (*Msg, error)
	//mockNats.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, nil)                               // Subscribe(subj string, cb MsgHandler) (*Subscription, error)
	//mockNats.On("QueueSubscribe", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, nil)                               // QueueSubscribe(subj string, queueGroup string, cb MsgHandler) (*Subscription, error)
	//mockNats.On("Drain").Return(nil) // Drain() error
	//mockNats.On("Close").Return()    // Close()
}

// TearDownSuite will be run by testify once at the end of the test suite after all tests have been executed.
func (suite *NatsClientTestSuite) TearDownSuite() {
	err := suite.natsClient.Close()
	assert.Nil(suite.T(), err)

	suite.mockNats.On("Drain").Return(nil) // Drain() error
	suite.mockNats.On("Close").Return()    // Close()

	err = suite.badNatsClient.Close()
	assert.Nil(suite.T(), err)
}

// SetupTest will be run before each test in the suite.
func (suite *NatsClientTestSuite) SetupTest() {
	suite.ctx, suite.ctxCancel = context.WithCancel(context.Background())
}

// TearDownTest will be run after each test in the suite.
func (suite *NatsClientTestSuite) TearDownTest() {
	suite.ctxCancel()
}

// TestExampleTestSuite runs the test suite.
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(NatsClientTestSuite))
}

// Test_PingPong tests ping-pong communication.
func (suite *NatsClientTestSuite) Test_PingPong() {
	const (
		subj     = "Test_PingPong"
		timeout1 = time.Millisecond
		timeout2 = time.Second
	)
	var result bool

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc()
	result, err := suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping should return an error")
	assert.False(suite.T(), result, "Ping should return false")

	pongSubscription, err := suite.natsClient.PongHandler(subj)
	assert.Nil(suite.T(), err, "PongHandler should not return an error")
	assert.NotNil(suite.T(), pongSubscription, "pongSubscription should not be nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc2()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping should not return an error")
	assert.True(suite.T(), result, "Ping should return true")

	err = pongSubscription.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription.Unsubscribe() should not return an error")

	ctx, cancelFunc3 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc3()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping should return an error")
	assert.False(suite.T(), result, "Ping should return false")
}

// Test_PongQueueGroup tests queue group pong handling.
func (suite *NatsClientTestSuite) Test_PongQueueGroup() {
	const (
		subj     = "Test_PongQueueGroup"
		queue    = subj
		timeout1 = time.Millisecond
		timeout2 = time.Second
	)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc()
	result, err := suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping should return an error")
	assert.False(suite.T(), result, "Ping should return false")

	pongSubscription, err := suite.natsClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler should not return an error")
	assert.NotNil(suite.T(), pongSubscription, "pongSubscription should not be nil")

	pongSubscription2, err := suite.natsClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler should not return an error")
	assert.NotNil(suite.T(), pongSubscription2, "pongSubscription2 should not be nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc2()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping should not return an error")
	assert.True(suite.T(), result, "Ping should return true")

	err = pongSubscription.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription.Unsubscribe() should not return an error")

	ctx, cancelFunc3 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc3()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping should not return an error")
	assert.True(suite.T(), result, "Ping should return true")

	err = pongSubscription2.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription2.Unsubscribe() should not return an error")

	ctx, cancelFunc4 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc4()
	result, err = suite.natsClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping should return an error")
	assert.False(suite.T(), result, "Ping should return false")
}

// Test_RequestReply tests the request-reply pattern.
func (suite *NatsClientTestSuite) Test_RequestReply() {
	const (
		cntMsg             = 3
		subj               = "Test_RequestReply"
		timeoutNatsRequest = time.Millisecond * 500
		timeoutTest        = time.Millisecond * 1000
		requestData        = "mock request data"
		replyData          = "mock reply data"
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

	respondersWG := sync.WaitGroup{}
	respondersWG.Add(1)

	// Server-side: simulate sending a request to the client
	go func(sendChan <-chan m.DataMock) {
		respondersWG.Wait()

		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.Nil(suite.T(), err, "Request should not return an error")
			assert.Equal(suite.T(), replyData, string(reply.Data), "Reply data mismatch")
		}
	}(sendChan)

	// Client-side: reply to the server
	go func() {
		routineWG := sync.WaitGroup{}
		routineWG.Add(cntMsg)

		respondersWG.Done()

		_, err := suite.natsClient.ReplyHandler(subj, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "Request data should not be nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "Incorrect request data received")
			}
			routineWG.Done()
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		assert.Nil(suite.T(), err, "Reply handler should not return an error")
		routineWG.Wait()
	}()

	wg.Wait()
	time.Sleep(timeoutTest)
}

func (suite *NatsClientTestSuite) Test_RequestReplyQueue() {
	const (
		cntMsg             = 3
		subj               = "Test_RequestReplyQueue"
		qGroup             = "Test_RequestReplyQueue"
		timeoutNatsRequest = time.Millisecond * 500
		requestData        = "mock request data"
		replyData          = "mock reply data"
		timeoutTest        = time.Millisecond * 1000
	)

	var cnt atomic.Int32 // Ensure that only one of the two handlers processes each request
	cnt.Store(0)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	var wg sync.WaitGroup
	sendChan := make(chan m.DataMock)
	wg.Add(cntMsg)

	// Server-side: simulate sending a request to the client
	go func(sendChan <-chan m.DataMock) {
		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.Nil(suite.T(), err, "Request should not return an error")
			assert.Equal(suite.T(), replyData, string(reply.Data), "Reply data mismatch")
		}
	}(sendChan)

	// Client-side: reply to the server
	go func() {
		routineWG := sync.WaitGroup{}
		routineWG.Add(cntMsg)

		s, err := suite.natsClient.ReplyQueueHandler(subj, qGroup, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "Request data should not be nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "Incorrect request data received")
			}
			cnt.Inc()
			routineWG.Done()
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		defer func() { assert.Nil(suite.T(), s.Unsubscribe(), "Unsubscribe should not return an error") }()
		assert.Nil(suite.T(), err, "Reply handler should not return an error")

		s2, err := suite.natsClient.ReplyQueueHandler(subj, qGroup, &m.DataMock{}, func(msg *Msg, request Serializable) Serializable {
			if example, ok := request.(*m.DataMock); ok {
				assert.NotNil(suite.T(), example, "Request data should not be nil")
				assert.Equal(suite.T(), requestData, string(example.Data), "Incorrect request data received")
			}
			cnt.Inc()
			routineWG.Done()
			wg.Done()
			return &m.DataMock{Data: []byte(replyData)}
		})
		defer func() { assert.Nil(suite.T(), s2.Unsubscribe(), "Unsubscribe should not return an error") }()
		assert.Nil(suite.T(), err, "Reply handler should not return an error")

		routineWG.Wait()
	}()

	for i := 0; i < cntMsg; i++ {
		sendChan <- m.DataMock{Data: []byte(requestData)}
	}
	close(sendChan)

	wg.Wait()
	assert.Equal(suite.T(), cntMsg, int(cnt.Load()), "Count mismatch")
	time.Sleep(timeoutTest)
}

func (suite *NatsClientTestSuite) Test_BadRequestReply() {
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

	// Server-side: simulate sending a bad request to the client
	go func(sendChan <-chan m.BadDataMock, done chan<- struct{}) {
		reply := &m.DataMock{}
		for request := range sendChan {
			request := request
			err := suite.natsClient.Request(ctx, subj, &request, reply)
			assert.NotNil(suite.T(), err, "Must return an error")
			assert.ErrorIs(suite.T(), err, m.ErrBadDataMock, "Error should match ErrBadDataMock")
		}
		done <- struct{}{}
	}(sendChan, done)

	<-done
	close(done)
}

func (suite *NatsClientTestSuite) Test_BadSubscribe() {
	errBad := errors.New("bad")
	suite.mockNats.On("Subscribe", mock.AnythingOfType("string"), mock.AnythingOfType("MsgHandler")).Return(nil, errBad) // Subscribe(subj string, cb MsgHandler) (*Subscription, error)

	s, err := suite.badNatsClient.ReplyHandler("testSubj", &m.DataMock{}, func(_ *Msg, _ Serializable) Serializable {
		return &m.DataMock{}
	})
	assert.Nil(suite.T(), s, "Subscription should be nil")
	assert.NotNil(suite.T(), err, "Must return an error")
	assert.ErrorIs(suite.T(), err, errBad, "Error should match errBad")
}

func (suite *NatsClientTestSuite) Test_BadRequest1() {
	const timeoutNatsRequest = time.Second
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeoutNatsRequest)
	defer cancelFunc()

	const errStrBad = "bad"
	errBad := errors.New(errStrBad)
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(&Msg{}, errBad)
	err := suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "Must return an error")
	assert.ErrorIs(suite.T(), err, errBad, "Error should match errBad")

	// It's a workaround since you can't change the hook with On() function
	suite.mockNats = &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = suite.mockNats
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil, nil)
	err = suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "Must return an error")
	assert.ErrorIs(suite.T(), err, ErrEmptyMsg, "Error should match ErrEmptyMsg")

	// It's a workaround since you can't change the hook with On() function
	suite.mockNats = &mocks.PureNatsConnI{}
	suite.badNatsClient.conn = suite.mockNats
	suite.mockNats.On("RequestWithContext", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(nil, errBad)
	err = suite.badNatsClient.Request(ctx, "testSubj", &m.DataMock{Data: []byte("mock data")}, &m.DataMock{})
	assert.NotNil(suite.T(), err, "Must return an error")
	assert.ErrorIs(suite.T(), err, errBad, "Error should match errBad")
}
