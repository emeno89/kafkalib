package kafka

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
)

type ConsumeHandlerFn func(ctx context.Context, kafkaMessage Message) error

type consumerGroupHandler struct {
	handlerFn          ConsumeHandlerFn
	groupId            string
	logger             Logger
	commitAfterFailure bool
}

func (cgh *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	cgh.logger.Info("[consume_handler] Consume setup", &MainLogInfo{GroupId: cgh.groupId})

	return nil
}

func (cgh *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (cgh *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		cgh.runHandler(sess, msg)
	}

	return nil
}

func (cgh *consumerGroupHandler) runHandler(sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	ctx := sess.Context()

	logInfo := &OffsetInfo{
		MainLogInfo: MainLogInfo{
			JsonMess: string(msg.Value),
			GroupId:  cgh.groupId,
		},
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}

	cgh.logger.Debug("[consume_handler] received", logInfo)

	var kafkaMessage Message

	if err := json.Unmarshal(msg.Value, &kafkaMessage); err != nil {
		cgh.logger.Error("[consume_handler] err", err, logInfo)
		return
	}

	if err := cgh.handlerFn(ctx, kafkaMessage); err != nil {
		cgh.logger.Error("[consume_handler] err", err, logInfo)
		if !cgh.commitAfterFailure {
			return
		}
	}

	sess.MarkMessage(msg, "")

	return
}
