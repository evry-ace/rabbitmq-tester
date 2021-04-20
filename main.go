package main

import (
	"flag"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var rmqAddr = flag.String("rabbitmq-address","amqp://guest:guest@localhost:5672/","The RabbitMQ address")
var exchangeName = flag.String("exchange-name","rbmq-test","The name of the exchange to publish to / consume messages from")
var isProducer = flag.Bool("is-producer", true, "Default true, if false type == consumer")
var testInterval = flag.Int("test-interval-seconds", 5, "Interval between messages beeing produced/consumed")

func main() {
	flag.Parse()

	if *isProducer {
		producer()
	} else {
		consumer()
	}
}

func setupConnection() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(*rmqAddr)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %s", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %s", err)
	}

	err = ch.ExchangeDeclare(
		*exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to declare an exchange: %s", err)
	}
	return conn, ch, err
}

func producer() {
	conn, ch, err := setupConnection()
	if err != nil {
		log.Println(err)
	}else{
		defer conn.Close()
		defer ch.Close()
	}
	
	ticker := time.NewTicker(time.Duration(float64(*testInterval)) * time.Second)
	quit := make(chan struct{})
	forever := make(chan bool)

	go func() {
		for {
			select {
			case <-ticker.C:
				err = ch.Publish(
					*exchangeName,
					"",
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte("hello world!"),
					})
			case <-quit:
				conn.Close()
				ch.Close()
				ticker.Stop()
				return
			}
		}
	}()
	<-forever
}

func consumer() {
	conn, ch, err := setupConnection()
	if err != nil {
		log.Printf("setupConnection failed: %s",err)
	}else {
		defer conn.Close()
		defer ch.Close()
	}
	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to declare queue: %s", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		*exchangeName,
		false,
		nil,
	)

	if err != nil {
		log.Printf("Failed to bind queue to exchange: %s", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to register a consumer: %s", err)
	}
	for d := range msgs {
		log.Printf("Message recived: %s",d.Body)
		d.Ack(false)
	}
}
