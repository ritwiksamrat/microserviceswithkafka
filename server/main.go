package main

import (
	"context"
	"fmt"
	"net"
	"encoding/json"
	"github.com/ritwiksamrat/microserviceswithkafka/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type server struct{}

func main() {

	fmt.Println("Initializing")
	fmt.Println("Server has started")
	listener, err := net.Listen("tcp", ":4040")
	if err != nil {
		panic(err.Error())
	}
	srv := grpc.NewServer()
	proto.RegisterKafkaservServer(srv, &server{})
	reflection.Register(srv)

	if e := srv.Serve(listener); e != nil {
		panic(err)
	}
	
}

 type model struct{

	 usesub string
	 useval string
 }

func (s *server) Kafservice(ctx context.Context, request *proto.Request) (*proto.Response, error) {

	key := request.GetSub()
	value := request.GetVal()

	pa:=&model{key,value}
	out, err := json.Marshal(pa)
    if err != nil {
        panic (err)
    }
//	producemap:=map[string]string{
//		key: value,
//	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	topic := "sampleTopic"

	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(string(out)),
	}, nil)

	p.Flush(15 * 1000)

	return &proto.Response{Result: "Success"}, nil
}
