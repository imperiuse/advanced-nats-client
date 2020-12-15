package main

import (
	"fmt"
	"os"
	"os/signal"

	pb "github.com/imperiuse/advance-nats-client/serializable/protobuf"

	nc "github.com/imperiuse/advance-nats-client/nats"
)

// Secondly, run NATS (you can do it f.e. by `make test_env_up`)
func main() {
	client, err := nc.New(nc.DefaultDSN)
	defer func() { fmt.Println("client Close()", client.Close()) }()
	if err != nil || client == nil {
		fmt.Println("Something went wrong! nc.New; err:", err)
		return
	}

	var subj = "subj"

	// here server side emulate send request to client
	sub, err := client.ReplyHandler(subj, &pb.Example_Request{}, func(_ *nc.Msg, data nc.Serializable) nc.Serializable {
		return &pb.Example_Response{
			Status: true,
			Desc:   "Ok!",
		}
	})
	defer func() { _ = sub.Unsubscribe() }()
	if err != nil {
		fmt.Println("Something went wrong! client.ReplyHandler(); err:", err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	s := <-c
	fmt.Println("Got signal: Exit", s)
}
