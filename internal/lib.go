package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// внутренняя структура, то как данные хранятся в памяти
type Notification struct {
	Id       string    `json:"id"`
	Msg      string    `json:"message"`
	SendDate time.Time `json:"send_date"`
	Status   string
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
// а это уже для API слоя
type Response struct {
	Id       string    `json:"id"`
	Msg      string    `json:"message"`
	SendDate time.Time `json:"send_date"`
	Status   string
}

func HandleCreateNotification(w http.ResponseWriter, r *http.Request) {
	// пришёл POST-запрос на создание уведомления /notify
	var msg string
	var ntf Notification
	json.NewDecoder(r.Body).Decode(&ntf)
	defer r.Body.Close()
	storage.mu.Lock()
	fmt.Println(ntf)
	InfoCh <- ntf.Id
	if _, ok := storage.notifications[ntf.Id]; !ok {
		storage.notifications[ntf.Id] = &ntf
		msg = "Created " + ntf.Id + " notification at " + ntf.SendDate.String()
		w.WriteHeader(http.StatusCreated)
	} else {
		msg = "Not created " + ntf.Id + " notification. Already exists."
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
		resp := Response{
			ntf.Id,
			ntf.Msg,
			ntf.SendDate,
			ntf.Status,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(&resp)
		msg = "Found " + id + " notification. Time info " + ntf.SendDate.String()

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
