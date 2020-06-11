package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/smartystreets/messaging/v3/rabbitmq"
	"github.com/smartystreets/messaging/v3/streaming"
)

func main() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
	logger := log.New(log.Writer(), log.Prefix(), log.Flags())
	connector := rabbitmq.New(
		rabbitmq.Options.Logger(logger),
	)

	reader := streaming.New(
		connector,
		streaming.Options.Logger(logger),
		streaming.Options.Subscriptions(
			streaming.NewSubscription("queue-name",
				streaming.SubscriptionOptions.AddWorkers(nopHandler{}),
				streaming.SubscriptionOptions.FullDeliveryToHandler(true),
			),
		),
	)

	go func() {
		channel := make(chan os.Signal, 3)
		signal.Notify(channel, syscall.SIGTERM, syscall.SIGINT)
		log.Printf("[INFO] Shutdown signal received [%s]", <-channel)
		signal.Stop(channel)
		_ = reader.Close()
	}()

	reader.Listen()
	log.Println("Reader concluded listening...")
}

type nopHandler struct{}

func (this nopHandler) Handle(context.Context, ...interface{}) {}
