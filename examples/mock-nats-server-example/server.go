package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	pb "github.com/imperiuse/advance-nats-client/serializable/protobuf"

	nc "github.com/imperiuse/advance-nats-client/nats"
)

// Secondly, run NATS (you can do it f.e. by `make test_env_up`)
func main() {
	client, err := nc.New(nc.DefaultDSN)
	defer func() { fmt.Println("client Close()", client.Close()) }()
	if err != nil || client == nil {
		fmt.Println("Something went wrong! New client")
		return
	}

	var subj nc.Subj = "subj"

	// here server side emulate send request to client
	go func() {
		for {
			data := &pb.Example_Request{
				Id:    time.Now().UnixNano(),
				Name:  "Example_Request",
				Price: 123,
			}

			res := &pb.Reply{}

			fmt.Println("Server send data")
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
			if err = client.Request(ctx, subj, data, res); err != nil {
				fmt.Println("Something went wrong! Server request", err)
				cancelFunc()
				return
			}
			cancelFunc()
			fmt.Printf("Server received reply: %+v \n", res)

			time.Sleep(time.Second * 10)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	s := <-c
	fmt.Println("Got signal: Exit", s)
}
