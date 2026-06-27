package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"notifier-service/internal/repository"
	"notifier-service/internal/service"
)

type handler struct {
	service *service.NotifyService
}

func NewHandler(service *service.NotifyService) *handler {
	return &handler{service: service}
}

func (h *handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /notify", h.HandleCreateNotification)
	mux.HandleFunc("GET /notify/{id}", h.HandleGetNotification)
	mux.HandleFunc("DELETE /notify/{id}", h.HandleDeleteNotification)
}

/*
Прописываем хэндлеры
*/

func (h *handler) HandleCreateNotification(w http.ResponseWriter, r *http.Request) {
	// пришёл POST-запрос на создание уведомления /notify
	// сначала нужно распарсить тело, потом собрать из него ntf и передать его в сервисный слой
	var ntf repository.Notification
	json.NewDecoder(r.Body).Decode(&ntf)
	defer r.Body.Close()
	// отдельно в хранилище
	ntf, err := h.service.CreateNotification(ntf)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusConflict)
		return
	}
	// отдельно в брокер
	ntf, err = h.service.PushNotificationToBroker(r.Context(), ntf) // добавим контекст
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *handler) HandleGetNotification(w http.ResponseWriter, r *http.Request) {
	// GET /notify/{id}
	idStr := r.PathValue("id")
	ntf, err := h.service.GetNotification(idStr)
	if errors.Is(err, repository.ErrNotFound) {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	} else if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	err = json.NewEncoder(w).Encode(&ntf)
	if err != nil {
		log.Println(err) // просто прокинем лог
	}
}

func (h *handler) HandleDeleteNotification(w http.ResponseWriter, r *http.Request) {
	// DELETE /notify/{id}
	idStr := r.PathValue("id")
	err := h.service.DeleteNotification(idStr)
	if errors.Is(err, repository.ErrNotFound) {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound) // ??? уточнить про код ответа
	} else if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
