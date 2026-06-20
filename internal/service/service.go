package service

import "notifier-service/internal/repository"

type NotifyRepository interface {
	Create(ntf repository.Notification) repository.Notification
	Get(id string) (repository.Notification, error)
	Delete(id string) error
}

type NotifyService struct {
	repo NotifyRepository
}

func NewNotifyService(repo NotifyRepository) *NotifyService {
	return &NotifyService{repo: repo}
}

func (s *NotifyService) CreateNotification(ntf repository.Notification) (repository.Notification, error) {
	// пока заглушка. Надо подумать какие параметры передавать в функцию
	n := s.repo.Create(ntf)
	return n, nil
}

func (s *NotifyService) GetNotification(id string) (repository.Notification, error) {
	return s.repo.Get(id)
}

func (s *NotifyService) DeleteNotification(id string) error {
	return s.repo.Delete(id)
}
