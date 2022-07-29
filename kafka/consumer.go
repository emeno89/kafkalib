package kafka

import (
	"context"
	"github.com/Shopify/sarama"
)

const (
	StartFromFirstMessage = -2
	StartFromLastMessage  = -1
)

type Consumer interface {
	Consume(ctx context.Context, topicList []string, handlerFn ConsumeHandlerFn, commitAfterFailure bool) error
	Close() error
}

type consumer struct {
	saramaConsumerGroup sarama.ConsumerGroup
	groupId             string
	logger              Logger
}

func (c *consumer) Consume(
	ctx context.Context,
	topicList []string,
	handlerFn ConsumeHandlerFn,
	commitAfterFailure bool,
) error {
	var stop bool

	cgh := consumerGroupHandler{
		handlerFn:          handlerFn,
		groupId:            c.groupId,
		logger:             c.logger,
		commitAfterFailure: commitAfterFailure,
	}

	logInfo := &MainLogInfo{
		GroupId:   c.groupId,
		TopicList: topicList,
	}

	for {
		select {
		case <-ctx.Done():
			stop = true
		default:
			if err := c.saramaConsumerGroup.Consume(ctx, topicList, &cgh); err != nil {
				c.logger.Error("[consumer] Consume err", err, logInfo)
			}
		}
		if stop {
			break
		}
	}

	return c.Close()
}

func (c *consumer) Close() error {
	logInfo := &MainLogInfo{GroupId: c.groupId}

	err := c.saramaConsumerGroup.Close()
	if err != nil {
		c.logger.Error("[consumer] Close err", err, logInfo)
		return err
	} else {
		c.logger.Info("[consumer] Close ok", logInfo)
	}

	return err
}

func NewConsumer(kafkaHostList []string, groupId string, startFrom int64, logger Logger) Consumer {
	config := sarama.NewConfig()

	config.Version = sarama.V2_0_0_0
	config.Consumer.Return.Errors = true

	switch startFrom {
	case StartFromFirstMessage:
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case StartFromLastMessage:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		config.Consumer.Offsets.Initial = startFrom
	}

	logInfo := &MainLogInfo{
		HostList: kafkaHostList,
		GroupId:  groupId,
	}

	saramaConsumerGroup, err := sarama.NewConsumerGroup(kafkaHostList, groupId, config)

	if err != nil {
		logger.Fatal("[consumer] Init err", err, logInfo)
	}

	go func() {
		for err = range saramaConsumerGroup.Errors() {
			logger.Error("[consumer] Consume err", err, logInfo)
		}
	}()

	return &consumer{
		saramaConsumerGroup: saramaConsumerGroup,
		groupId:             groupId,
		logger:              logger,
	}
}
