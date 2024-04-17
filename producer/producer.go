package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"
	"sync"
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

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	panicOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	channl, err := conn.Channel()
	panicOnError(err, "couldn't get a channel")
	defer channl.Close()

	qu, err := channl.QueueDeclare("pending_transactions", true, false, false, false, nil)
	panicOnError(err, "failed to create a q")

	ctxt, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go (func(i int) {

			for j := 1; j <= 10; j++ {

				var buf bytes.Buffer
				currTxMessage := TxMessage{strconv.Itoa(i), strconv.Itoa(j)}
				buf.Reset()

				encoder := gob.NewEncoder(&buf)
				err := encoder.Encode(currTxMessage)
				if err != nil {
					fmt.Println(err)
				}
				fmt.Println(buf.Len())
				err = channl.PublishWithContext(ctxt, "", qu.Name, false, false, amqp.Publishing{ContentType: "application/octet-stream", Body: buf.Bytes(), DeliveryMode: amqp.Persistent})

				panicOnError(err, "cannot push to queue")
				fmt.Printf("sent transaction from sender %s , txID: %s \n", currTxMessage.SenderID, currTxMessage.TxNonce)
			}
		})(i)
	}

	wg.Wait()

}
