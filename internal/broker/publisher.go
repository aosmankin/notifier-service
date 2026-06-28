package broker

import (
	"context"
	"log"
	"notifier-service/internal/repository"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/rabbitmq"
)

/*Клиент Rabbit будет один для publisher и consumer*/
type Publisher interface {
	PublishMsg(ctx context.Context, msg []byte) error
	PublishMsgWithDelay(ctx context.Context, d amqp091.Delivery, delay int64, headers amqp091.Table) error
}

// обёртка над библиотечным паблишером
type RabbitPublisher struct {
	publisher rabbitmq.Publisher
}

func NewRabbitPublisher(clt *rabbitmq.RabbitClient) *RabbitPublisher {
	pub := rabbitmq.NewPublisher(clt, "MyExchange", "application/json")
	return &RabbitPublisher{publisher: *pub}
}

func (rb *RabbitPublisher) PublishMsg(ctx context.Context, msg []byte) error {
	// контекст как будто лучше прокинуть в структуру publisher (нет хуйня)
	// routing key можно оставить прям на этом уровне
	/*хотя если их два, то возможно стоит указать его в аргументах функции*/
	err := rb.publisher.Publish(ctx, msg, "NotifyRoutingKey")
	if err != nil {
		log.Printf("Error in PublishMsg: %v\n", err)
		return err
	}
	return nil
}

func (rb *RabbitPublisher) PublishMsgWithDelay(ctx context.Context, d amqp091.Delivery, delay int64, newHeaders amqp091.Table) error {
	if delay < 1 {
		delay = 1
	}
	headers := d.Headers
	if headers == nil {
		headers = make(amqp091.Table)
	}
	for k, v := range newHeaders {
		headers[k] = v
	}
	err := rb.publisher.Publish(ctx, d.Body, "DelayRoutingKey", // DelayRoutingKey
		rabbitmq.WithHeaders(headers),
		rabbitmq.WithExpiration(time.Duration(delay)*time.Millisecond))
	return err
}

func SendNotificationToUser(ntf repository.Notification) error {
	// имитация отправления уведомления пользователю
	log.Printf("Notfication %s was sent to user at %v\n%s\n", ntf.Id, ntf.SendDate, ntf.Msg)
	return nil
}
