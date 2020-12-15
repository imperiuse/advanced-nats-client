package main

import (
	"context"
	"fmt"
	"time"

	nc "github.com/imperiuse/advance-nats-client/nats"
)

// Firstly run NATS (you can do it f.e. by `make test_env_up`)
func main() {
	client, err := nc.New(nc.DefaultDSN)
	defer func() { fmt.Println("Client Close()", client.Close()) }()
	if err != nil || client == nil {
		fmt.Println("Something went wrong! New Client")
		return
	}

	var pingSubj = "ping_topic"
	pongSubscription, err := client.PongHandler(pingSubj)
	if err != nil {
		fmt.Println("Something went wrong! RegisterPongHandlerRC()")
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc()
	var result bool
	result, err = client.Ping(ctx, pingSubj)
	fmt.Printf("Result of Ping: %v; err: %v\n", result, err) // should be `Result of Ping: true; <nil>`, if Ok

	fmt.Println("Unsubscribe from pongSubscription", pongSubscription.Unsubscribe()) // should be `Unsubscribe from pongSubscription <nil>`

	ctx, cancelFunc2 := context.WithTimeout(context.Background(), time.Second)
	defer cancelFunc2()
	result, err = client.Ping(ctx, pingSubj)
	fmt.Printf("Result of Ping: %v; err: %v\n", result, err) // should be `Result of Ping: flase; Empty msg, msg is nil: nats: timeout`, if Ok
}
