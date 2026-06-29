package repository

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("Notification not found")
)

// внутренняя структура, то как данные хранятся в памяти
type Notification struct {
	Id       string    `json:"id"`
	Msg      string    `json:"message"`
	SendDate time.Time `json:"send_date"`
	Status   string    `json:"status"`
}

type notifyRepository struct {
	storage map[string]Notification
	mu      sync.Mutex
}

func NewNotifyRepository() *notifyRepository {
	return &notifyRepository{
		make(map[string]Notification),
		sync.Mutex{},
	}
}

// Создаёт новое уведомление
func (r *notifyRepository) Create(ntf Notification) Notification {
	r.mu.Lock()
	defer r.mu.Unlock()

	// уточнить момент откуда приходит ID уведомления. Мы его сами создаём или оно нам уже приходит в качестве поля у notification
	r.storage[ntf.Id] = ntf
	return ntf
}

func (r *notifyRepository) Get(id string) (Notification, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ntf, ok := r.storage[id]
	if !ok {
		return Notification{}, ErrNotFound
	}
	return ntf, nil
}

func (r *notifyRepository) Delete(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.storage[id]; !ok {
		return ErrNotFound
	}

	delete(r.storage, id)
	return nil
}
