package main

import (
	"log"
	"github.com/streadway/amqp"
)


func main(){
		//consumer()
		producer()
}

func failOnError(err error, errMsg string){
	if err != nil {
		log.Fatalf("%s:,%s",errMsg,err)
	}
}

func setupConnection() (*amqp.Connection, *amqp.Channel, amqp.Queue ) {
	log.Println("Connecting to RabbitMQ")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err,"Failed to connect to RabbitMQ")
	
	log.Println("Opening a channel to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err,"Failed to open a channel")
	
	log.Println("Declaring a que")
	que, err := ch.QueueDeclare(
		"test_monitor_que",		//name
		false,								//durable
		false,								//delete when unused
		false,								//exclusive
		false,								//no-wait
		nil,									//arguments
	)
	failOnError(err, "Failed to declare a que")
	return conn,ch,que
}

func producer() {
	conn,ch,que := setupConnection()
	defer conn.Close()
	defer ch.Close()

	log.Println("Publishing a message")
	body := "Hello World!"
	err := ch.Publish(
		"",     		// exchange
		que.Name, 	// routing key
		false,  		// mandatory
		false,  		// immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
		})
		failOnError(err, "Failed to publish a message")
		log.Println("Message published")			
	}
	
	func consumer(){
		conn,ch,que := setupConnection()
		defer conn.Close()
		defer ch.Close()

		msgs, err := ch.Consume(
			que.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")
		
		forever := make(chan bool)

		go func() {
			for d := range msgs {
				log.Printf("Recived a message: %s", d.Body)
			}
		}()
		log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
		<-forever
	}