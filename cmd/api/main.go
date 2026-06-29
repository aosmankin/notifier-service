package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"notifier-service/internal/broker"
	"notifier-service/internal/config"
	"notifier-service/internal/handler"
	"notifier-service/internal/repository"
	"notifier-service/internal/service"

	"github.com/rabbitmq/amqp091-go"
	"github.com/wb-go/wbf/rabbitmq"
	"github.com/wb-go/wbf/retry"
)

func main() {
	envCfg, err := config.LoadEnvConfig()
	if err != nil {
		log.Fatalln(err)
	}

	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    3 * time.Second,
		Backoff:  2,
	}
	cfg := rabbitmq.ClientConfig{
		URL:            envCfg.RabbitURL,     // вынести в .env
		ConnectionName: envCfg.RabbitConName, // вынести в .env
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
		"x-dead-letter-routing-key": "NotifyRoutingKey",
	}
	err = clt.DeclareQueue("delay-notify-queue", "MyExchange", "DelayRoutingKey", true, false, true, delayArgs)
	if err != nil {
		log.Fatalf("Error by declare Delay Queue: %v\n", err)
	}
	// создаем publisher
	pub := broker.NewRabbitPublisher(clt)

	// создаём слои repository, service
	repo := repository.NewNotifyRepository()
	svc := service.NewNotifyService(repo, pub)

	// создаём consumer
	csm := broker.NewRabbitConsumer(clt, svc.ProcessQueuedNotification)

	// создаем слой handler
	h := handler.NewHandler(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	/*publisher будет на сервисном слое (не в repository). То есть логично создать
	в структуре NotifyService интерфейс Publisher */
	srv := http.Server{
		Addr:         envCfg.HostAddr, // адрес лучше прописать через переменные окружения
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
		service.CheckNotifications(context.Background(), csm)
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
