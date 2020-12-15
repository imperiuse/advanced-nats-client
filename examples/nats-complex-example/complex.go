package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	nc "github.com/imperiuse/advance-nats-client/nats"
	pb "github.com/imperiuse/advance-nats-client/serializable/protobuf"
)

// Don't forget run NATS (you can do it f.e. by `make test_env_up`)
func main() {
	client, err := nc.New(nc.DefaultDSN)
	defer func() { fmt.Println("client Close()", client.Close()) }()
	if err != nil || client == nil {
		fmt.Println("Something went wrong! New client")
		return
	}

	var id int64 = 1

	// here server side emulate send request to client
	go func() {
		for {
			subj := fmt.Sprintf("subj_%d", id)

			data := &pb.Example_Request{
				Id:    1,
				Name:  "2",
				Price: 3,
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

			time.Sleep(time.Second * 2)
		}
	}()

	// here client side reply to server
	go func() {
		subj := fmt.Sprintf("subj_%d", id)

		// VARIANT I   (I prefer this variant, it's most common and few code)
		if _, err := client.ReplyHandler(subj, &pb.Example_Request{}, func(_ *nc.Msg, request nc.Serializable) nc.Serializable {
			t := time.Now()
			if example, ok := request.(*pb.Example_Request); ok {
				//some work with protobuf obj
				fmt.Printf("I received data: %+v\n", example)
			}
			return &pb.Reply{SomeStuff: fmt.Sprintf("I: %v", time.Since(t).Nanoseconds())}
		}); err != nil {
			fmt.Println("Something went wrong! client.RegisterReplyHandler()", err)
		}

		//VARIANT II  (more fast variant (20%--30% faster then variant I), without type cast)
		//if _, err = client.NatsConn().Subscribe( subj,
		//	func(msg *nats.Msg) {
		//		t := time.Now()
		//		example := &pb.Example{}
		//		err := example.Unmarshal(msg.Data)
		//		if err != nil {
		//			fmt.Println(err)
		//		}
		//		//some work with protobuf obj
		//		fmt.Printf("I received data: %+v\n", example)
		//
		//data, err := (&pb.Reply{SomeStuff: fmt.Sprintf("II: %v", time.Since(t).Nanoseconds())}).Marshal()
		//if err != nil {
		//	fmt.Println(err)
		//}
		//err = msg.Respond(data)
		//if err != nil {
		//	fmt.Println(err)
		//}
		//},
		//); err != nil {
		//	fmt.Println("Something went wrong! client.RegisterReplyNatsHandler()", err)
		//}

		for {
			fmt.Println(".")
			time.Sleep(time.Second)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// Block until a signal is received.
	s := <-c
	fmt.Println("Got signal: Exit", s)
}
