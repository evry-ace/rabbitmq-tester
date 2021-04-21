package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"
)

var (
	failed     = "failed"
	success    = "success"
	promStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "rabbitmq_test",
			Name:      "produce_consume",
		},
		[]string{"kind", "status", "node"},
	)
)

var addr = flag.String("listen-address", "127.0.0.1:8080", "The address to listen on for HTTP requests (expose metrics)")
var rmqAddr = flag.String("rabbitmq-address", "amqp://guest:guest@localhost:5672/", "The RabbitMQ address")
var exchangeName = flag.String("exchange-name", "rbmq-testt", "The name of the exchange to publish to / consume messages from")
var isProducer = flag.Bool("is-producer", true, "Default true, if false type == consumer")
var testInterval = flag.Int("test-interval-seconds", 10, "Interval between messages beeing produced/consumed")
var nodeName = flag.String("node-name", "localhost", "The name of the node running the container")

func init() {
	prometheus.MustRegister(promStatus)
}

func main() {
	flag.Parse()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}()

	if *isProducer {
		for {
			tryAgainOnFailure := producer()
			if !tryAgainOnFailure {
				break
			}
			time.Sleep(time.Duration(*testInterval) * time.Second)
		}
	} else {
		for {
			tryAgainOnFailure := consumer()
			if !tryAgainOnFailure {
				break
			}
			time.Sleep(time.Duration(*testInterval) * time.Second)
		}
	}
}

func setupConnection() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(*rmqAddr)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %s", err)
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Failed to open a channel: %s", err)
		return nil, nil, err
	}

	err = ch.ExchangeDeclare(*exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Printf("Failed to declare an exchange: %s", err)
		return nil, nil, err
	}
	return conn, ch, err
}

func producer() bool {
	tryAgainOnFailure := true
	kind := "producer"
	conn, ch, err := setupConnection()
	if err != nil {
		updateMetrics(kind, failed, success)
		return tryAgainOnFailure
	}
	defer conn.Close()
	defer ch.Close()
	ticker := time.NewTicker(time.Duration(float64(*testInterval)) * time.Second)
	quit := make(chan bool)
	log.Printf("Ready to publish messages")
	go func() {
		for {
			select {
			case <-ticker.C:
				err = ch.Publish(*exchangeName, "", false, false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte("hello world!"),
					})
				if err != nil {
					log.Printf("Failed to publish message: %s", err)
					updateMetrics(kind, failed, success)
					quit <- true
				} else {
					log.Printf("Published message")
					updateMetrics(kind, success, failed)
				}
			case <-quit:
				return
			}
		}
	}()
	<-quit
	return tryAgainOnFailure
}

func consumer() bool {
	tryAgainOnFailure := true
	kind := "consumer"
	conn, ch, err := setupConnection()
	if err != nil {
		updateMetrics(kind, failed, success)
		return tryAgainOnFailure
	}
	defer conn.Close()
	defer ch.Close()
	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		log.Printf("Failed to declare queue: %s", err)
		updateMetrics(kind, failed, success)
		return tryAgainOnFailure
	}

	err = ch.QueueBind(q.Name, "", *exchangeName, false, nil)
	if err != nil {
		log.Printf("Failed to bind queue to exchange: %s", err)
		updateMetrics(kind, failed, success)
		return tryAgainOnFailure
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Failed to register a consumer: %s", err)
		updateMetrics(kind, failed, success)
		return tryAgainOnFailure
	}
	log.Printf("Ready to recive messages")
	updateMetrics(kind, success, failed)
	for d := range msgs {
		log.Printf("Message recived: %s", d.Body)
		err = d.Ack(false)
		if err != nil{
			log.Printf("Ack error: %s",err)
			break
		}
	}
	updateMetrics(kind, failed, success)
	return tryAgainOnFailure
}

func updateMetrics(kind string, currStatus string, prevStatus string) {
	promStatus.With(prometheus.Labels{"kind": kind, "status": currStatus, "node": *nodeName}).Set(1)
	promStatus.With(prometheus.Labels{"kind": kind, "status": prevStatus, "node": *nodeName}).Set(0)
}
