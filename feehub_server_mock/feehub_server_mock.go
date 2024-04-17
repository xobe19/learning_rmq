package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func panicOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type TxMessage struct {
	SenderID string
	TxNonce  string
}

func main() {
	var buf bytes.Buffer
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	panicOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channl, err := conn.Channel()
	panicOnError(err, "couldn't get a channel")
	defer channl.Close()

	qu, err := channl.QueueDeclare("pending_transactions", true, false, false, false, nil)
	panicOnError(err, "failed to create a q")

	msgs, err := channl.Consume(qu.Name, "", false, false, false, false, nil)

	panicOnError(err, "cannot consumer from queue")
	var forever chan struct{}

	go func() {
		for d := range msgs {
			buf.Reset()
			buf.Write(d.Body)
			fmt.Print(d.DeliveryMode, d.ContentType, len(d.Body))
			var txMessage TxMessage

			decoder := gob.NewDecoder(&buf)
			decoder.Decode(&txMessage)
			fmt.Println(txMessage)

			log.Printf("received transaction from node %s with nonce %s", txMessage.SenderID, txMessage.TxNonce)
			time.Sleep(200 * time.Millisecond)
			log.Printf("processed transaction from node %s with nonce %s", txMessage.SenderID, txMessage.TxNonce)
			d.Ack(false)
		}
	}()
	log.Printf("server started...")
	<-forever

}
