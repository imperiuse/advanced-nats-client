package streaming

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	mock2 "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	nc "github.com/imperiuse/advance-nats-client/nats"
	"github.com/imperiuse/advance-nats-client/serializable/mock"
	"github.com/imperiuse/advance-nats-client/streaming/mocks"
)

var testDSN = []URL{"nats://127.0.0.1:4223"}

type NatsStreamingClientTestSuit struct {
	suite.Suite
	ctx                context.Context
	ctxCancel          context.CancelFunc
	streamingClient    AdvanceNatsClient
	badStreamingClient *client
	mockStanConn       *mocks.PureNatsStunConnI
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (suite *NatsStreamingClientTestSuit) SetupSuite() {
	c, err := NewOnlyStreaming("bad_name_cluster", "test-client", []URL{"1.2.3.4:1234"})
	assert.NotNil(suite.T(), err, "must be error!")
	assert.Nil(suite.T(), c, "must be nil!")

	c, err = NewOnlyStreaming("bad_name_cluster", "test-client", testDSN)
	assert.NotNil(suite.T(), err, "must be error!")
	assert.Nil(suite.T(), c, "must be nil!")

	c, err = NewOnlyStreaming(DefaultClusterID, fmt.Sprint(uuid.Must(uuid.NewV4())), testDSN)
	assert.Nil(suite.T(), err, "err must be nil!")
	assert.NotNil(suite.T(), c, "client be not nil!")
	assert.Nil(suite.T(), c.nc, "nc must be nil!")
	assert.Nil(suite.T(), c.NatsConn(), "NatsConn nc must be nil!")
	assert.Nil(suite.T(), c.Nats(), "Nats nc must be nil!")

	assert.Nil(suite.T(), c.Close(), "close problem")

	badNatsClient := nc.NewDefaultClient()
	c1, err := New(DefaultClusterID, fmt.Sprint(uuid.Must(uuid.NewV4())), badNatsClient)
	assert.Equal(suite.T(), ErrNilNatsConn, err)
	assert.Nil(suite.T(), c1, "client must be nil!")

	natsClient, err := nc.New(testDSN)
	assert.Nil(suite.T(), err, "err must be nil")
	assert.NotNil(suite.T(), natsClient, "natsClient must be non nil")

	suite.streamingClient, err = New(DefaultClusterID, fmt.Sprint(uuid.Must(uuid.NewV4())), natsClient)
	assert.Nil(suite.T(), err, "err must be nil!")
	assert.NotNil(suite.T(), suite.streamingClient, "client be not nil!")
	assert.NotNil(suite.T(), suite.streamingClient.NatsConn(), "NatsConn nc must not be nil!")
	assert.NotNil(suite.T(), suite.streamingClient.Nats(), "Nats nc must not be nil!")
	suite.streamingClient.UseCustomLogger(zap.NewNop())

	suite.badStreamingClient, err = New(DefaultClusterID, fmt.Sprint(uuid.Must(uuid.NewV4())), natsClient)
	suite.badStreamingClient.UseCustomLogger(zap.NewNop())
	assert.NotNil(suite.T(), suite.badStreamingClient, "client must be non nil")
	assert.Nil(suite.T(), err, "err must be nil")
	mockStanConn := &mocks.PureNatsStunConnI{}
	suite.badStreamingClient.sc = mockStanConn
	suite.mockStanConn = mockStanConn

	// Mock configurations down
	// NB: .Return(...) must return the same signature as the method being mocked.
	//mockStanConn.On("Close").Return(nil)    // Close()error

}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (suite *NatsStreamingClientTestSuit) TearDownSuite() {
	err := suite.streamingClient.Close()
	assert.Nil(suite.T(), err)

	suite.mockStanConn.On("Close").Return(nil) // Close()

	err = suite.badStreamingClient.Close()
	assert.Nil(suite.T(), err)
}

// The SetupTest method will be run before every test in the suite.
func (suite *NatsStreamingClientTestSuit) SetupTest() {
	suite.ctx, suite.ctxCancel = context.WithCancel(context.Background())
}

// The TearDownTest method will be run after every test in the suite.
func (suite *NatsStreamingClientTestSuit) TearDownTest() {
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestExampleTestSuite(t *testing.T) {
	suite.Run(t, new(NatsStreamingClientTestSuit))
}

//All methods that begin with "Test" are run as tests within a suite.
func (suite *NatsStreamingClientTestSuit) Test_PublishSync() {
	err := suite.streamingClient.PublishSync("Test_PublishSync", &mock.DataMock{Data: []byte("test_data")})
	assert.Nil(suite.T(), err, "PublishSync err")

	err = suite.streamingClient.PublishSync("Test_PublishSync", &mock.BadDataMock{})
	assert.NotNil(suite.T(), err, "PublishSync must return err")
	assert.Equal(suite.T(), mock.ErrBadDataMock, err)
}

func (suite *NatsStreamingClientTestSuit) Test_PublishAsync() {
	guid, err := suite.streamingClient.PublishAsync("Test_PublishAsync", &mock.DataMock{Data: []byte("test_data")}, nil)
	assert.Nil(suite.T(), err, "PublishAsync err")
	assert.NotEqual(suite.T(), EmptyGUID, guid, "GUID must not be empty")

	guid, err = suite.streamingClient.PublishAsync("Test_PublishAsync", &mock.BadDataMock{}, nil)
	assert.NotNil(suite.T(), err, "PublishAsync must return err")
	assert.Equal(suite.T(), mock.ErrBadDataMock, err)
	assert.Equal(suite.T(), EmptyGUID, guid, "GUID must be empty")
}

func (suite *NatsStreamingClientTestSuit) Test_Subscribe() {
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
		assert.True(t, ok, "can't type cast")
		wg.Done()
	}
	s, err := suite.streamingClient.Subscribe(subj, &mock.DataMock{}, handler)
	defer func() { _ = s.Close() }()
	assert.Nil(t, err, "err must be nil")

	wg.Add(cntMsg * 2)
	for i := 0; i < cntMsg; i++ {
		err := suite.streamingClient.PublishSync(subj, &mock.DataMock{Data: []byte(testData)})
		assert.Nil(t, err, "PublishSync err")

		guid, err := suite.streamingClient.PublishAsync(subj, &mock.DataMock{Data: []byte(testData)}, nil)
		assert.Nil(t, err, "PublishAsync err")
		assert.NotEqual(t, EmptyGUID, guid, "GUID must not be empty")
	}

	wg.Wait()
}

func (suite *NatsStreamingClientTestSuit) Test_BadPublish() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("Publish", mock2.AnythingOfType("string"), mock2.Anything).Return(errBad)
	suite.mockStanConn.On("PublishAsync", mock2.AnythingOfType("string"), mock2.Anything, mock2.Anything).Return(EmptyGUID, errBad)

	err := suite.badStreamingClient.PublishSync("testSubj", &mock.DataMock{Data: []byte("test_data")})
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, err, "must be equals")

	guid, err := suite.badStreamingClient.PublishAsync("testSubj", &mock.DataMock{Data: []byte("test_data")}, suite.badStreamingClient.DefaultAckHandler())
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, err, "must be equals")
	assert.Equal(suite.T(), EmptyGUID, guid, "must be equals")
}

func (suite *NatsStreamingClientTestSuit) Test_BadSubscribe() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("Subscribe", mock2.AnythingOfType("string"), mock2.AnythingOfType("MsgHandler"), mock2.AnythingOfType("SubscriptionOption")).Return(nil, errBad) // Subscribe(subj string, cb MsgHandler) (*Subscription, error)

	s, err := suite.badStreamingClient.Subscribe("testSubj", &mock.DataMock{}, EmptyHandler)
	assert.Nil(suite.T(), s, "must be nil")
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, err, "must be equals")
}

func (suite *NatsStreamingClientTestSuit) Test_PingPongDummyTest() {
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

func (suite *NatsStreamingClientTestSuit) Test_RequestDummyTest() {
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

func (suite *NatsStreamingClientTestSuit) Test_RequestQueueDummyTest() {
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

func (suite *NatsStreamingClientTestSuit) Test_CheckNilNatsClient() {
	c := client{}

	_, err := c.ReplyHandler("", &mock.DataMock{}, nil)
	assert.Equal(suite.T(), ErrNilNatsClient, err)

	err = c.Request(context.Background(), "", &mock.DataMock{}, &mock.DataMock{})
	assert.Equal(suite.T(), ErrNilNatsClient, err)

	_, err = c.PongHandler("")
	assert.Equal(suite.T(), ErrNilNatsClient, err)

	_, err = c.Ping(context.Background(), "")
	assert.Equal(suite.T(), ErrNilNatsClient, err)

	_, err = c.PongQueueHandler("", "")
	assert.Equal(suite.T(), ErrNilNatsClient, err)
}

func (suite *NatsStreamingClientTestSuit) Test_QueueSubscribe() {
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

func (suite *NatsStreamingClientTestSuit) Test_QueueSubscribeSerializable() {
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

func (suite *NatsStreamingClientTestSuit) Test_BadQueueSubscriber() {
	errBad := errors.New("bad")
	suite.mockStanConn.On("QueueSubscribe", mock2.AnythingOfType("string"), mock2.AnythingOfType("string"), mock2.Anything, mock2.Anything).Return(nil, errBad) // Subscribe(subj string, cb MsgHandler) (*Subscription, error)

	s, err := suite.badStreamingClient.QueueSubscribe("testSubj", "testQ", &mock.DataMock{}, EmptyHandler)
	assert.Nil(suite.T(), s, "must be nil")
	assert.NotNil(suite.T(), err, "must be err")
	assert.Equal(suite.T(), errBad, err, "must be equals")
}
