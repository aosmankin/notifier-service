package main

import (
	"fmt"
	"log"
	"net/http"
	"notifier-service/internal"
	"sync"
	"time"
)

func main() {
	_, err := internal.InitAll()
	if err != nil {
		log.Fatalf("failed initialization %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /notify", internal.HandleCreateNotification)
	mux.HandleFunc("GET /notify/{id}", internal.HandleGetNotification)
	mux.HandleFunc("DELETE /notify/{id}", internal.HandleDeleteNotification)
	srv := http.Server{
		Addr:         ":8080", // адрес лучше прописать через переменные окружения
		Handler:      mux,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		IdleTimeout:  20 * time.Millisecond,
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

	// пока просто для мониторинга
	defer close(internal.InfoCh)
	for str := range internal.InfoCh {
		fmt.Println(str)
	}

	wg.Wait()
}
