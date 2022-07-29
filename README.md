kafkalib
======

kafkalib implements wrapper for Shopify/sarama

# Installation
```
go get github.com/emeno89/kafkalib
```

# Quick Start

```go
package main

import (
	"context"
	"github.com/emeno89/kafkalib/kafka"
	"go.uber.org/zap"
	"sync"
	"time"
)

type kafkaLogger struct {
	logger *zap.Logger
}

func (s *kafkaLogger) Fatal(msg string, err error, info kafka.LogInfo) {
	s.logger.Fatal(msg, zap.Error(err), zap.String("info", info.String()))
}

func (s *kafkaLogger) Debug(msg string, info kafka.LogInfo) {
	s.logger.Debug(msg, zap.String("info", info.String()))
}

func (s *kafkaLogger) Info(msg string, info kafka.LogInfo) {
	s.logger.Info(msg, zap.String("info", info.String()))
}

func (s *kafkaLogger) Error(msg string, err error, info kafka.LogInfo) {
	s.logger.Error(msg, zap.Error(err), zap.String("info", info.String()))
}

func main() {
	hostList := []string{"localhost:9092"}

	zapLogger, _ := zap.NewDevelopment()

	kLogger := &kafkaLogger{logger: zapLogger}

	topicList := []string{"tests"}

	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		consumer := kafka.NewConsumer(hostList, "testConsumer", kafka.StartFromFirstMessage, kLogger)
		_ = consumer.Consume(ctx, topicList, func(ctx context.Context, kafkaMessage kafka.Message) error {
			zapLogger.Debug("Catch message", zap.Any("mess", kafkaMessage))
			return nil
		}, false)
	}()

	producer := kafka.NewAsyncProducer(hostList, time.Second, kLogger)

	producer.Produce(topicList[0], kafka.MakeMessage("test.event", "{\"id\": 10}"), "id10")

	time.Sleep(5 * time.Second)

	cancel()

	wg.Wait()

	_ = producer.Close()
}
````

In the example above we create simple async producer and consumer. Producer sends message and are going to close, Consumer reads this message and receives cancel signal.
You can implement your own handler (for example - struct method).
You can also send error from your handler and control processing of consumer using commitAfterFailure in Consume method. 