package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"notifier-service/internal/broker"
	"notifier-service/internal/repository"

	"github.com/rabbitmq/amqp091-go"
)

type NotifyRepository interface {
	Create(ntf repository.Notification) repository.Notification
	Get(id string) (repository.Notification, error)
	Delete(id string) error
}

type NotifyService struct {
	repo NotifyRepository
	pub  broker.Publisher
}

func NewNotifyService(repo NotifyRepository, pub broker.Publisher) *NotifyService {
	return &NotifyService{repo: repo, pub: pub}
}

func (s *NotifyService) CreateNotification(ntf repository.Notification) (repository.Notification, error) {
	// пока заглушка. Надо подумать какие параметры передавать в функцию
	n := s.repo.Create(ntf)
	return n, nil
}

/*пока разделим между собой отправку в хранилище и отправку в брокер*/
func (s *NotifyService) PushNotificationToBroker(ctx context.Context, ntf repository.Notification) (repository.Notification, error) {
	msg, err := json.Marshal(ntf)
	if err != nil {
		log.Printf("Unmarshal error in PushNotificationToBroker: %v\n", err)
		return ntf, err
	}
	err = s.pub.PublishMsg(ctx, msg)
	if err != nil {
		log.Printf("PublishMsg returns error %v\n", err)
		return ntf, err
	}
	return ntf, nil
}

func (s *NotifyService) GetNotification(id string) (repository.Notification, error) {
	return s.repo.Get(id)
}

func (s *NotifyService) DeleteNotification(id string) error {
	return s.repo.Delete(id)
}

func (s *NotifyService) ProcessQueuedNotification(ctx context.Context, d amqp091.Delivery) error {
	var ntf repository.Notification

	err := json.Unmarshal(d.Body, &ntf)
	if err != nil {
		log.Printf("cannot unmarshal notification in consumer: %v\n", err)
		return err
	}

	// проверяем, не было ли удалено сообщение из хранилища (map)
	storedNtf, err := s.repo.Get(ntf.Id)
	if errors.Is(err, repository.ErrNotFound) {
		log.Printf("Notification %s was deleted/canceled, skip sending\n", ntf.Id)
		return nil // ACK. Сообщение удалится из очереди Rabbit, но пользователю не уйдёт.
	}
	if err != nil {
		return err
	}

	ntf = storedNtf

	currTime := time.Now()

	if ntf.SendDate.After(currTime) {
		delay := ntf.SendDate.Sub(currTime).Milliseconds()

		err := s.pub.PublishMsgWithDelay(ctx, d, delay, d.Headers)
		if err != nil {
			return err
		}

		return nil // ACK старого сообщения
	}

	err = SendNotificationToUser(ntf)
	if err == nil {
		err = s.DeleteNotification(ntf.Id)
		if err != nil && !errors.Is(err, repository.ErrNotFound) {
			log.Printf("cannot delete notification %s after send: %v\n", ntf.Id, err)

			// Важно подумать, что возвращать.
			// Если вернуть err, сообщение может быть переотправлено пользователю повторно.
			return err
		}

		return nil // ACK
	}

	retryCount := getCountFromHeader(d.Headers)
	maxCount, _ := strconv.Atoi(os.Getenv("MAX_RETRIES")) // не оч

	if retryCount >= maxCount {
		log.Printf("Reached max retries. Notification id %s\n", ntf.Id)
		return nil // или отправить в dead-letter, но не бесконечно ретраить
	}

	newDelay := time.Millisecond * time.Duration(1<<retryCount)
	newHeaders := amqp091.Table{
		"x-retry-count": retryCount + 1,
	}

	err = s.pub.PublishMsgWithDelay(ctx, d, int64(newDelay), newHeaders)
	if err != nil {
		return err
	}

	return nil
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

// background-функция для проверки очереди
func CheckNotifications(ctx context.Context, rc *broker.RabbitConsumer) {
	if err := rc.Consumer.Start(ctx); err != nil {
		log.Fatalf("Message consuming error: %v\n", err)
	}
}
func SendNotificationToUser(ntf repository.Notification) error {
	// имитация отправления уведомления пользователю
	log.Printf("Notfication %s was sent to user at %v\n%s\n", ntf.Id, ntf.SendDate, ntf.Msg)
	return nil
}
