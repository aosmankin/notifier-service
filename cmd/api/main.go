package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"notifier-service/internal/broker"
	"notifier-service/internal/handler"
	"notifier-service/internal/repository"
	"notifier-service/internal/service"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/rabbitmq"
	"github.com/wb-go/wbf/retry"
)

func main() {
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    3 * time.Second,
		Backoff:  2,
	}
	cfg := rabbitmq.ClientConfig{
		URL:            "amqp://root:root_password@localhost:5673/", // вынести в .env
		ConnectionName: "notification-service",                      // вынести в .env
		ConnectTimeout: 5 * time.Second,
		Heartbeat:      10 * time.Second,
		ProducingStrat: strategy,
		ConsumingStrat: strategy,
	}
	clt, err := rabbitmq.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error by creating RabbitClient: %v\n", err)
	}
	// объявляем обменник и очереди: основную и TTL
	err = clt.DeclareExchange("MyExchange", "direct", true, false, false, nil)
	if err != nil {
		log.Fatalf("Error by declareExchange: %v\n", err)
	}
	err = clt.DeclareQueue("notify-queue", "MyExchange", "NotifyRoutingKey", true, false, true, nil)
	if err != nil {
		log.Fatalf("Error by declare main Queue: %v\n", err)
	}
	delayArgs := amqp091.Table{
		"x-dead-letter-exchange":    "MyExchange",
		"x-dead-letter-routing-key": "notify",
	}
	err = clt.DeclareQueue("delay-notify-queue", "MyExchange", "DelayRoutingKey", true, false, true, delayArgs)
	if err != nil {
		log.Fatalf("Error by declare Delay Queue: %v\n", err)
	}
	// создаем publisher и consumer
	pub := broker.NewRabbitPublisher(clt)
	csm := broker.NewRabbitConsumer(clt, pub)

	// создаём слои repository, service и handler
	repo := repository.NewNotifyRepository()
	svc := service.NewNotifyService(repo, pub)
	h := handler.NewHandler(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	/*publisher будет на сервисном слое (не в repository). То есть логично создать
	в структуре NotifyService интерфейс Publisher */
	srv := http.Server{
		Addr:         ":8080", // адрес лучше прописать через переменные окружения
		Handler:      mux,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		IdleTimeout:  30 * time.Millisecond,
	}
	var wg sync.WaitGroup
	// запускаем checkout очереди
	wg.Add(1)
	go func() {
		defer wg.Done()
		broker.CheckNotifications(context.Background(), csm)
	}()

	// запускаем http-сервер
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
