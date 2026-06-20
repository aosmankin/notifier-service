package main

import (
	"fmt"
	"log"
	"net/http"
	"notifier-service/internal/handler"
	"notifier-service/internal/repository"
	"notifier-service/internal/service"
	"sync"
	"time"
)

func main() {
	repo := repository.NewNotifyRepository()
	svc := service.NewNotifyService(repo)
	h := handler.NewHandler(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	srv := http.Server{
		Addr:         ":8080", // адрес лучше прописать через переменные окружения
		Handler:      mux,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		IdleTimeout:  30 * time.Millisecond,
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Server started at", srv.Addr)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	wg.Wait()
}
