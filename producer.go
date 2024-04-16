package main

import (
	"context"
	"log"
	"time"

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

	ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = channl.PublishWithContext(ctxt, "", qu.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("Hello world")})

	panicOnError(err, "cannot push to queue")
	log.Printf("sent")

}
