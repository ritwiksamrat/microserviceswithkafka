package main

import (
	"context"
	"fmt"
	"net"

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
func (s *server) Kafservice(ctx context.Context, request *proto.Request) (*proto.Response, error) {

	key := request.GetSub()
	value := request.GetVal()

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

	topic := "myTopic"
	var uname string = key
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(string(uname)),
	}, nil)

	p.Flush(15 * 1000)

	//SECOND PRODUCER:-
	p1, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	defer p1.Close()

	go func() {
		for er := range p.Events() {
			switch evs := er.(type) {
			case *kafka.Message:
				if evs.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", evs.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", evs.TopicPartition)
				}
			}
		}
	}()

	topic1 := "SampleTopic"
	var acv string = value
	p1.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic1, Partition: kafka.PartitionAny},
		Value:          []byte(string(acv)),
	}, nil)

	p1.Flush(15 * 1000)

	return &proto.Response{Result: "Success"}, nil
}
