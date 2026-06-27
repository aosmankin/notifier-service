package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"notifier-service/internal/repository"
	"strconv"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/rabbitmq"
)

type MsgConsumer interface {
	Consume(ctx context.Context) error
}

type RabbitConsumer struct {
	consumer rabbitmq.Consumer
	pub      Publisher
}

func NewRabbitConsumer(clt *rabbitmq.RabbitClient, pub Publisher) *RabbitConsumer {
	//args := amqp091.Table{}
	consumerCfg := rabbitmq.ConsumerConfig{
		Queue: "notify-queue",
		//Args:  args,
	}
	handler := func(ctx context.Context, d amqp091.Delivery) error {
		// Обработка...
		/*здесь будет основная логика программы*/
		var ntf repository.Notification
		err := json.Unmarshal(d.Body, &ntf)
		if err != nil {
			log.Printf("cannot unmarshal ntf in consumer: %v\n", err)
			return err
		}

		currTime := time.Now()
		if ntf.SendDate.After(currTime) {
			// отправлям в очередь с задержкой
			delay := ntf.SendDate.Sub(currTime).Milliseconds()
			err := pub.PublishMsgWithDelay(ctx, d, delay, d.Headers)
			return err
		}

		err = SendNotificationToUser(ntf)
		if err == nil {
			return nil // ACK
		}

		retryCount := getCountFromHeader(d.Headers)
		maxCount := 5
		if retryCount >= maxCount {
			log.Printf("Reached max retries. Notification id %s\n", ntf.Id)
		}

		newDelay := time.Millisecond * time.Duration(1<<retryCount)
		newHeaders := amqp091.Table{"x-retry-count": retryCount + 1}
		err = pub.PublishMsgWithDelay(ctx, d, int64(newDelay), newHeaders)
		return err // вернуть ошибку, если нужно NACK
	}
	csm := rabbitmq.NewConsumer(clt, consumerCfg, handler)
	return &RabbitConsumer{consumer: *csm, pub: pub}
}

// background-функция для проверки очереди
func CheckNotifications(ctx context.Context, rc *RabbitConsumer) {
	if err := rc.consumer.Start(ctx); err != nil {
		log.Fatalf("Message consuming error: %v\n", err)
	}
}

func getCountFromHeader(headers amqp091.Table) int {
	if headers == nil {
		return 0
	}
	if v, ok := headers["x-retry-count"]; ok {
		if i, err := strconv.Atoi(fmt.Sprint(v)); err == nil {
			return i
		}
	}
	return 0
}
