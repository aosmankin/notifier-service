package internal

import (
	"net/http"
	"sync"
	"time"
)

type Notification struct {
	id     string
	info   time.Time
	status int
}

type Storage struct {
	notifications map[string]*Notification
	mu            sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		make(map[string]*Notification),
		sync.RWMutex{},
	}
}

// функция инициализации
func InitAll() (*Storage, error) {
	storage := NewStorage()
	return storage, nil
}

// пока напишем глобальный Storage. Потом подумаю, как правильно изолировать
var storage = Storage{
	make(map[string]*Notification),
	sync.RWMutex{},
}

var InfoCh = make(chan string, 5) // создали глобальный канал для мониторинга

/*
Прописываем хэндлеры
*/

func HandleCreateNotification(w http.ResponseWriter, r *http.Request) {
	// пришёл POST-запрос на создание уведомления /notify
	var msg string
	id := r.URL.Query().Get("id")
	storage.mu.Lock()
	if _, ok := storage.notifications[id]; !ok {
		ntf := &Notification{
			id,
			time.Now(),
			0,
		}
		storage.notifications[id] = ntf
		msg = "Created " + id + " notification at " + ntf.info.String()
		w.WriteHeader(http.StatusCreated)
	} else {
		msg = "Not created " + id + " notification. Already exists."
		w.WriteHeader(http.StatusConflict)
	}
	storage.mu.Unlock()
	InfoCh <- msg
}

func HandleGetNotification(w http.ResponseWriter, r *http.Request) {
	var msg string
	id := r.PathValue("id")
	storage.mu.RLock()
	if _, ok := storage.notifications[id]; !ok {
		w.WriteHeader(http.StatusNotFound)
		msg = "Not found " + id + " notification"
	} else {
		ntf := storage.notifications[id]
		w.WriteHeader(http.StatusOK)
		msg = "Found " + id + " notification. Time info " + ntf.info.String()
	}
	storage.mu.RUnlock()
	InfoCh <- msg
}

func HandleDeleteNotification(w http.ResponseWriter, r *http.Request) {
	var msg string
	id := r.PathValue("id")
	storage.mu.Lock()
	if _, ok := storage.notifications[id]; !ok {
		w.WriteHeader(http.StatusNotFound)
		msg = "Can not delete " + id + " notification. Not found"
	} else {
		delete(storage.notifications, id)
		w.WriteHeader(http.StatusOK)
		msg = "Delete successful " + id + " notification."
	}
	storage.mu.Unlock()
	InfoCh <- msg
}
