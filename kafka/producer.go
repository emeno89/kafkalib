package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"time"
)

type Producer interface {
	Produce(topic string, kafkaMessage Message, key string) (partition int32, offset int64, err error)
	Close() error
}

type producer struct {
	saramaProducer sarama.SyncProducer
	logger         Logger
}

func (p *producer) Produce(topic string, kafkaMessage Message, key string) (partition int32, offset int64, err error) {
	jsonMess, _ := json.Marshal(kafkaMessage)

	producerMessage := sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(jsonMess),
		Key:   sarama.StringEncoder(key),
	}

	logInfo := LogInfo{
		TopicList: []string{topic},
		Key:       key,
		JsonMess:  jsonMess,
		Partition: partition,
		Offset:    offset,
	}

	partition, offset, err = p.saramaProducer.SendMessage(&producerMessage)
	if err != nil {
		p.logger.Error("[sync_producer] Produce err", err, logInfo)
	} else {
		p.logger.Debug("[sync_producer] Produce ok", logInfo)
	}

	return partition, offset, err
}

func (p *producer) Close() error {
	logInfo := LogInfo{}

	err := p.saramaProducer.Close()
	if err != nil {
		p.logger.Error("[sync_producer] Close err", err, logInfo)
	} else {
		p.logger.Info("[sync_producer] Close ok", logInfo)
	}

	return err
}

func NewProducer(kafkaHostList []string, logger Logger) Producer {
	logInfo := LogInfo{
		HostList: kafkaHostList,
	}

	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 300 * time.Millisecond
	config.Producer.Return.Successes = true

	saramaProducer, err := sarama.NewSyncProducer(kafkaHostList, config)
	if err != nil {
		logger.Fatal("[sync_producer] Init err", err, logInfo)
	}

	return &producer{
		saramaProducer: saramaProducer,
		logger:         logger,
	}
}
