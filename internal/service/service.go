package service

import (
	"context"
	"encoding/json"
	"log"

	"notifier-service/internal/broker"
	"notifier-service/internal/repository"
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
