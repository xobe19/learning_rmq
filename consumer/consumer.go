package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func panicOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	panicOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channl, err := conn.Channel()
	panicOnError(err, "couldn't get a channel")
	defer channl.Close()

	qu, err := channl.QueueDeclare("msgs", false, false, false, false, nil)
	panicOnError(err, "failed to create a q")

	msgs, err := channl.Consume(qu.Name, "", true, false, false, false, nil)

	panicOnError(err, "cannot consumer from queue")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("msg rec: %s", d.Body)
		}
	}()
	log.Printf("message waiting")
	<-forever

}
