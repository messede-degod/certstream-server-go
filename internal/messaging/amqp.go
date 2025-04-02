package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var messagingAvailable = false
var messageQueue amqp.Queue
var messageChannel *amqp.Channel
var MQMU sync.Mutex = sync.Mutex{}
var appId string

func Initialize(AMQPServerAddress, AMQPQueueName, AppId string) {
	appId = AppId

	conn, err := amqp.Dial(AMQPServerAddress)
	if err != nil {
		log.Println("InitializeMessaging() : ", err)
		return
	}

	messageChannel, err = conn.Channel()
	if err != nil {
		log.Println("InitializeMessaging() : ", err)
		return
	}

	messageQueue, err = messageChannel.QueueDeclare(
		AMQPQueueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Println("InitializeMessaging() : ", err)
		return
	}

	messagingAvailable = true
	log.Println("Connected to MQ!")

	return
}

func SendMessage(message any) error {
	messageBytes, mberr := json.Marshal(message)
	if mberr == nil {
		return SendMessageBytes(messageBytes)
	}
	return mberr
}

func SendMessageBytes(message []byte) error {
	MQMU.Lock()
	defer MQMU.Unlock()

	if !messagingAvailable {
		return errors.New("messaging is unavailable")
	}

	return messageChannel.PublishWithContext(context.Background(),
		"",                // exchange
		messageQueue.Name, // routing key
		false,             // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/json",
			Body:         message,
			AppId:        appId,
		})
}
