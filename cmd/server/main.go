package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"notifier-service/internal"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/rabbitmq"
	"github.com/wb-go/wbf/retry"
)

func main() {
	// давай просто проинициализируем rabbit прям здесь. Чтобы всё работало. А потом уже вынесем в отдельный модуль
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    3 * time.Second,
		Backoff:  2,
	}
	cfg := rabbitmq.ClientConfig{
		URL:            "amqp://root:root_password@localhost:5672/",
		ConnectionName: "notification-service",
		ConnectTimeout: 5 * time.Second,
		Heartbeat:      10 * time.Second,
		ProducingStrat: strategy,
		ConsumingStrat: strategy,
	}
	client, err := rabbitmq.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error of connecting to RabbitMQ: %v", err)
	}
	defer client.Close()

	publisher := rabbitmq.NewPublisher(client, "MyNotifyExchange", "application/json")

	ctx := context.Background()

	bodyMsg := []byte(`{"event":"user_registered","id":123}`)
	routingKey := "MyTestRoutingKey"
	err = publisher.Publish(
		ctx,
		bodyMsg,
		routingKey,
		rabbitmq.WithExpiration(5*time.Minute),
		rabbitmq.WithHeaders(amqp091.Table{"x-service": "auth"}),
	)
	if err != nil {
		log.Printf("Ошибка публикации: %v", err)
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
