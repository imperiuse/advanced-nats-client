package streaming

// TODO INTEGRATION: MUST CREATE TESTS FOR RECONNECT BEHAVIOR

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	nc "github.com/imperiuse/advanced-nats-client/v1/nats"
	"github.com/imperiuse/advanced-nats-client/v1/serializable/mock"
	"github.com/imperiuse/advanced-nats-client/v1/streaming/mocks"
	"github.com/imperiuse/advanced-nats-client/v1/uuid"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var testDSN = []URL{"nats://127.0.0.1:4223"}

type NatsStreamingClientTestSuite struct {
	suite.Suite
	ctx                context.Context
	ctxCancel          context.CancelFunc
	streamingClient    AdvancedNatsClient
	badStreamingClient *client
	mockStanConn       *mocks.PureNatsStunConnI
}

// SetupSuite is called once, before any tests in the suite are run.
func (suite *NatsStreamingClientTestSuite) SetupSuite() {
	c, err := NewOnlyStreaming("bad_name_cluster", uuid.UUID4(), []URL{"1.2.3.4:1234"})
	assert.NotNil(suite.T(), err, "must be an error!")
	assert.Nil(suite.T(), c, "must be nil!")

	c, err = NewOnlyStreaming("bad_name_cluster", uuid.UUID4(), testDSN)
	assert.NotNil(suite.T(), err, "must be an error!")
	assert.Nil(suite.T(), c, "must be nil!")

	c, err = NewOnlyStreaming(DefaultClusterID, uuid.UUID4(), testDSN)
	assert.Nil(suite.T(), err, "error must be nil!")
	assert.NotNil(suite.T(), c, "client must not be nil!")
	assert.Nil(suite.T(), c.nc, "nc must be nil!")
	assert.Nil(suite.T(), c.NatsConn(), "NatsConn nc must be nil!")
	assert.Nil(suite.T(), c.Nats(), "Nats nc must be nil!")

	assert.Nil(suite.T(), c.Close(), "close problem")

	badNatsClient := nc.NewDefaultClient()
	c1, err := New(DefaultClusterID, uuid.UUID4(), badNatsClient)
	assert.ErrorIs(suite.T(), err, ErrNilNatsConn)
	assert.Nil(suite.T(), c1, "client must be nil!")

	natsClient, err := nc.New(testDSN)
	assert.Nil(suite.T(), err, "error must be nil")
	assert.NotNil(suite.T(), natsClient, "natsClient must not be nil")

	suite.streamingClient, err = New(DefaultClusterID, uuid.UUID4(), natsClient)
	assert.Nil(suite.T(), err, "error must be nil!")
	assert.NotNil(suite.T(), suite.streamingClient, "client must not be nil!")
	assert.NotNil(suite.T(), suite.streamingClient.NatsConn(), "NatsConn nc must not be nil!")
	assert.NotNil(suite.T(), suite.streamingClient.Nats(), "Nats nc must not be nil!")
	suite.streamingClient.UseCustomLogger(zap.NewNop())

	suite.badStreamingClient, err = New(DefaultClusterID, uuid.UUID4(), natsClient)
	suite.badStreamingClient.UseCustomLogger(zap.NewNop())
	assert.NotNil(suite.T(), suite.badStreamingClient, "client must not be nil")
	assert.Nil(suite.T(), err, "error must be nil")
	mockStanConn := &mocks.PureNatsStunConnI{}
	suite.badStreamingClient.sc = mockStanConn
	suite.mockStanConn = mockStanConn
}

// TearDownSuite is called once, after all tests in the suite are run.
func (suite *NatsStreamingClientTestSuite) TearDownSuite() {
	err := suite.streamingClient.Close()
	assert.Nil(suite.T(), err)

	suite.mockStanConn.On("Close").Return(nil) // Close()

	err = suite.badStreamingClient.Close()
	assert.Nil(suite.T(), err)
}

// SetupTest is called before each test.
func (suite *NatsStreamingClientTestSuite) SetupTest() {
	suite.ctx, suite.ctxCancel = context.WithCancel(context.Background())
}

// TearDownTest is called after each test.
func (suite *NatsStreamingClientTestSuite) TearDownTest() {
}

// TestExampleTestSuite runs the test suite.
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(NatsStreamingClientTestSuite))
}

// Test_PublishSync checks synchronous publishing.
func (suite *NatsStreamingClientTestSuite) Test_PublishSync() {
	err := suite.streamingClient.PublishSync("Test_PublishSync", &mock.DataMock{Data: []byte("test_data")})
	assert.Nil(suite.T(), err, "PublishSync error")

	err = suite.streamingClient.PublishSync("Test_PublishSync", &mock.BadDataMock{})
	assert.NotNil(suite.T(), err, "PublishSync must return an error")
	assert.ErrorIs(suite.T(), err, mock.ErrBadDataMock)
}

// Test_PublishAsync checks asynchronous publishing.
func (suite *NatsStreamingClientTestSuite) Test_PublishAsync() {
	guid, err := suite.streamingClient.PublishAsync("Test_PublishAsync", &mock.DataMock{Data: []byte("test_data")}, nil)
	assert.Nil(suite.T(), err, "PublishAsync error")
	assert.NotEqual(suite.T(), EmptyGUID, guid, "GUID must not be empty")

	guid, err = suite.streamingClient.PublishAsync("Test_PublishAsync", &mock.BadDataMock{}, nil)
	assert.NotNil(suite.T(), err, "PublishAsync must return an error")
	assert.ErrorIs(suite.T(), err, mock.ErrBadDataMock)
	assert.Equal(suite.T(), EmptyGUID, guid, "GUID must be empty")
}

// Test_Subscribe checks subscription functionality.
func (suite *NatsStreamingClientTestSuite) Test_Subscribe() {
	const (
		cntMsg   = 5
		subj     = "Test_Subscribe"
		testData = "testData"
	)
	var wg sync.WaitGroup

	t := suite.T()

	handler := func(_ *Msg, data Serializable) {
		v, ok := data.(*mock.DataMock)
		if ok {
			assert.Equal(t, testData, string(v.Data))
		}
		assert.True(t, ok, "cannot type cast")
		wg.Done()
	}
	s, err := suite.streamingClient.Subscribe(subj, &mock.DataMock{}, handler)
	defer func() { _ = s.Close() }()
	assert.Nil(t, err, "error must be nil")

	wg.Add(cntMsg * 2)
	for i := 0; i < cntMsg; i++ {
		err := suite.streamingClient.PublishSync(subj, &mock.DataMock{Data: []byte(testData)})
		assert.Nil(t, err, "PublishSync error")

		guid, err := suite.streamingClient.PublishAsync(subj, &mock.DataMock{Data: []byte(testData)}, nil)
		assert.Nil(t, err, "PublishAsync error")
		assert.NotEqual(t, EmptyGUID, guid, "GUID must not be empty")
	}

	wg.Wait()
}

// Test_BadPublish tests publishing when there is an error.
func (suite *NatsStreamingClientTestSuite) Test_BadPublish() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("Publish", mock2.AnythingOfType("string"), mock2.Anything).Return(errBad)
	suite.mockStanConn.On("PublishAsync", mock2.AnythingOfType("string"), mock2.Anything, mock2.Anything).Return(EmptyGUID, errBad)

	err := suite.badStreamingClient.PublishSync("testSubj", &mock.DataMock{Data: []byte("test_data")})
	assert.NotNil(suite.T(), err, "must be an error")
	assert.ErrorIs(suite.T(), err, errBad, "must be equal")

	guid, err := suite.badStreamingClient.PublishAsync("testSubj", &mock.DataMock{Data: []byte("test_data")}, suite.badStreamingClient.DefaultAckHandler())
	assert.NotNil(suite.T(), err, "must be an error")
	assert.Equal(suite.T(), errBad, err, "must be equal")
	assert.Equal(suite.T(), EmptyGUID, guid, "must be equal")
}

// Test_BadSubscribe tests subscription with an error.
func (suite *NatsStreamingClientTestSuite) Test_BadSubscribe() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("Subscribe", mock2.AnythingOfType("string"), mock2.AnythingOfType("MsgHandler"), mock2.AnythingOfType("SubscriptionOption")).Return(nil, errBad)

	s, err := suite.badStreamingClient.Subscribe("testSubj", &mock.DataMock{}, EmptyHandler)
	assert.Nil(suite.T(), s, "must be nil")
	assert.NotNil(suite.T(), err, "must be an error")
	assert.Equal(suite.T(), err, errBad, "must be equal")
}

// Test_PingPongDummyTest tests ping-pong functionality.
func (suite *NatsStreamingClientTestSuite) Test_PingPongDummyTest() {
	const (
		subj     = "Test_PingPongDummyTest"
		queue    = subj
		timeout1 = time.Millisecond
		timeout2 = time.Second
	)
	var result bool

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc()
	result, err := suite.streamingClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")

	pongSubscription, err := suite.streamingClient.PongHandler(subj)
	assert.Nil(suite.T(), err, "PongHandler error")
	assert.NotNil(suite.T(), pongSubscription, "pongSubscription must non nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc2()
	result, err = suite.streamingClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription.Unsubscribe() err")

	ctx, cancelFunc3 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc3()
	result, err = suite.streamingClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")

	pongSubscription2, err := suite.streamingClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler error")
	assert.NotNil(suite.T(), pongSubscription2, "pongSubscription2 must non nil")

	pongSubscription3, err := suite.streamingClient.PongQueueHandler(subj, queue)
	assert.Nil(suite.T(), err, "PongQueueHandler error")
	assert.NotNil(suite.T(), pongSubscription3, "pongSubscription3 must non nil")

	ctx, cancelFunc4 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc4()
	result, err = suite.streamingClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription2.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription2.Unsubscribe() err")

	ctx, cancelFunc5 := context.WithTimeout(context.Background(), timeout2)
	defer cancelFunc5()
	result, err = suite.streamingClient.Ping(ctx, subj)
	assert.Nil(suite.T(), err, "Ping err")
	assert.True(suite.T(), result, "Ping be true")

	err = pongSubscription3.Unsubscribe()
	assert.Nil(suite.T(), err, "pongSubscription3.Unsubscribe() err")

	ctx, cancelFunc6 := context.WithTimeout(context.Background(), timeout1)
	defer cancelFunc6()
	result, err = suite.streamingClient.Ping(ctx, subj)
	assert.NotNil(suite.T(), err, "Ping err")
	assert.False(suite.T(), result, "Ping must be false")
}

func (suite *NatsStreamingClientTestSuite) Test_RequestDummyTest() {
	const (
		timeout = time.Second
		subj    = "Test_RequestDummyTest"
	)
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	err := suite.streamingClient.Request(ctx, subj, &mock.DataMock{}, &mock.DataMock{})
	assert.NotNil(suite.T(), err, "must be err")

	var wg sync.WaitGroup
	wg.Add(1)
	s, err := suite.streamingClient.ReplyHandler(subj, &mock.DataMock{}, func(msg *nc.Msg, serializable nc.Serializable) nc.Serializable {
		wg.Done()
		return &mock.DataMock{}
	})
	defer func() { assert.Nil(suite.T(), s.Unsubscribe(), "must be nil") }()
	assert.NotNil(suite.T(), s, "client must be not bil")
	assert.Nil(suite.T(), err, "err must be nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc2()
	err = suite.streamingClient.Request(ctx, subj, &mock.DataMock{}, &mock.DataMock{})
	assert.Nil(suite.T(), err, "err must be nil")

	wg.Wait()
}

func (suite *NatsStreamingClientTestSuite) Test_RequestQueueDummyTest() {
	const (
		timeout = time.Second
		subj    = "Test_RequestQueueDummyTest"
		qGroup  = "Test_RequestQueueDummyTest"
	)
	var cnt atomic.Int32
	cnt.Store(0)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	err := suite.streamingClient.Request(ctx, subj, &mock.DataMock{}, &mock.DataMock{})
	assert.NotNil(suite.T(), err, "must be err")

	var wg sync.WaitGroup
	wg.Add(1)
	s, err := suite.streamingClient.ReplyQueueHandler(subj, qGroup, &mock.DataMock{}, func(msg *nc.Msg, serializable nc.Serializable) nc.Serializable {
		cnt.Inc()
		wg.Done()
		return &mock.DataMock{}
	})
	defer func() { assert.Nil(suite.T(), s.Unsubscribe(), "must be nil") }()
	assert.NotNil(suite.T(), s, "client must be not bil")
	assert.Nil(suite.T(), err, "err must be nil")

	s2, err := suite.streamingClient.ReplyQueueHandler(subj, qGroup, &mock.DataMock{}, func(msg *nc.Msg, serializable nc.Serializable) nc.Serializable {
		cnt.Inc()
		wg.Done()
		return &mock.DataMock{}
	})
	defer func() { assert.Nil(suite.T(), s2.Unsubscribe(), "must be nil") }()
	assert.NotNil(suite.T(), s2, "client must be not bil")
	assert.Nil(suite.T(), err, "err must be nil")

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc2()
	err = suite.streamingClient.Request(ctx, subj, &mock.DataMock{}, &mock.DataMock{})
	assert.Nil(suite.T(), err, "err must be nil")

	wg.Wait()

	assert.Equal(suite.T(), 1, int(cnt.Load()))
}

func (suite *NatsStreamingClientTestSuite) Test_CheckNilNatsClient() {
	c := client{}

	_, err := c.ReplyHandler("", &mock.DataMock{}, nil)
	assert.ErrorIs(suite.T(), err, ErrNilNatsClient)

	err = c.Request(context.Background(), "", &mock.DataMock{}, &mock.DataMock{})
	assert.ErrorIs(suite.T(), err, ErrNilNatsClient)

	_, err = c.PongHandler("")
	assert.ErrorIs(suite.T(), err, ErrNilNatsClient)

	_, err = c.Ping(context.Background(), "")
	assert.ErrorIs(suite.T(), err, ErrNilNatsClient)

	_, err = c.PongQueueHandler("", "")
	assert.ErrorIs(suite.T(), err, ErrNilNatsClient)
}

func (suite *NatsStreamingClientTestSuite) Test_QueueSubscribe() {
	const (
		cntMsg = 5
		data   = "data"
		subj   = "Test_QueueSubscribe"
	)

	var wgQ1, wgQ2, wgQ3 sync.WaitGroup
	mh1 := func(msg *Msg, _ Serializable) {
		//fmt.Printf("MH_1_Q1: %v\n", msg)
		wgQ1.Done()
	}
	mh2 := func(msg *Msg, _ Serializable) {
		//fmt.Printf("MH_2_Q1: %v\n", msg)
		wgQ1.Done()
	}
	mh3 := func(msg *Msg, _ Serializable) {
		//fmt.Printf("MH_3_Q2: %v\n", msg)
		wgQ2.Done()
	}
	mh4 := func(msg *Msg, _ Serializable) {
		//fmt.Printf("MH_4_Q3: %v\n", msg)
		wgQ3.Done()
	}
	mh5 := func(msg *Msg, _ Serializable) {
		//fmt.Printf("MH_5_Q3: %v\n", msg)
		wgQ3.Done()
	}
	s, err := suite.streamingClient.QueueSubscribe(subj, "Q1", &mock.DataMock{}, mh1)
	assert.Nil(suite.T(), err, "must be nil")
	defer func() { assert.Nil(suite.T(), s.Close(), "must be nil") }()

	s2, err := suite.streamingClient.QueueSubscribe(subj, "Q1", &mock.DataMock{}, mh2)
	assert.Nil(suite.T(), err, "must be nil")
	defer func() { assert.Nil(suite.T(), s2.Close(), "must be nil") }()

	s3, err := suite.streamingClient.QueueSubscribe(subj, "Q2", &mock.DataMock{}, mh3)
	assert.Nil(suite.T(), err, "must be nil")
	defer func() { assert.Nil(suite.T(), s3.Close(), "must be nil") }()

	s4, err := suite.streamingClient.QueueSubscribe(subj, "Q3", &mock.DataMock{}, mh4)
	assert.Nil(suite.T(), err, "must be nil")
	defer func() { assert.Nil(suite.T(), s4.Close(), "must be nil") }()

	s5, err := suite.streamingClient.QueueSubscribe(subj, "Q3", &mock.DataMock{}, mh5)
	assert.Nil(suite.T(), err, "must be nil")
	defer func() { assert.Nil(suite.T(), s5.Close(), "must be nil") }()

	wgQ1.Add(cntMsg)
	wgQ2.Add(cntMsg)
	wgQ3.Add(cntMsg)
	for i := 0; i < cntMsg; i++ {
		err = suite.streamingClient.PublishSync(subj, &mock.DataMock{
			Data: []byte(data + "_" + fmt.Sprint(i)),
		})
		assert.Nil(suite.T(), err, "must be nil")
	}

	wgQ1.Wait()
	wgQ2.Wait()
	wgQ3.Wait()
}

func (suite *NatsStreamingClientTestSuite) Test_QueueSubscribeSerializable() {
	const (
		cntMsg = 5
		data   = "data"
		subj   = "Test_QueueSubscribeSerializable"
	)

	t := suite.T()

	var wg sync.WaitGroup
	var cnt int
	mh := func(msg *Msg, v Serializable) {
		d, ok := v.(*mock.DataMock)
		assert.Equal(t, data+"_"+fmt.Sprint(cnt), string(d.Data))
		cnt++
		assert.True(t, ok)
		wg.Done()
	}

	var res mock.DataMock
	s, err := suite.streamingClient.QueueSubscribe(subj, "Q1", &res, mh)
	assert.Nil(t, err, "must be nil")
	defer func() { assert.Nil(suite.T(), s.Close(), "must be nil") }()

	wg.Add(cntMsg)
	for i := 0; i < cntMsg; i++ {
		err = suite.streamingClient.PublishSync(subj,
			&mock.DataMock{
				Data: []byte(data + "_" + fmt.Sprint(i)),
			})
		assert.Nil(suite.T(), err, "must be nil")
	}

	wg.Wait()
}

func (suite *NatsStreamingClientTestSuite) Test_BadQueueSubscriber() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("QueueSubscribe", mock2.AnythingOfType("string"), mock2.AnythingOfType("string"), mock2.Anything, mock2.Anything).Return(nil, errBad) // Subscribe(subj string, cb MsgHandler) (*Subscription, error)

	s, err := suite.badStreamingClient.QueueSubscribe("testSubj", "testQ", &mock.DataMock{}, EmptyHandler)
	assert.Nil(suite.T(), s, "must be nil")
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), err, errBad, "must be equals")
}
