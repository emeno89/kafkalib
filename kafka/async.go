package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"time"
)

type AsyncProducer interface {
	Produce(topic string, kafkaMessage Message, key string)
	Close() error
}

type asyncProducer struct {
	saramaProducer sarama.AsyncProducer
	waitCloseTime  time.Duration
	logger         Logger
}

func NewAsyncProducer(kafkaHostList []string, waitCloseTime time.Duration, logger Logger) AsyncProducer {
	logInfo := &MainLogInfo{
		HostList: kafkaHostList,
	}

	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 300 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	saramaProducer, err := sarama.NewAsyncProducer(kafkaHostList, config)
	if err != nil {
		logger.Fatal("[async_producer] Init err", err, logInfo)
	}

	obj := &asyncProducer{
		saramaProducer: saramaProducer,
		waitCloseTime:  waitCloseTime,
		logger:         logger,
	}

	go obj.readSuccesses()
	go obj.readErrors()

	return obj
}

func (p *asyncProducer) Produce(topic string, kafkaMessage Message, key string) {
	jsonMess, _ := json.Marshal(kafkaMessage)

	p.saramaProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(jsonMess),
		Key:   sarama.StringEncoder(key),
	}

	logInfo := &MainLogInfo{
		JsonMess:  string(jsonMess),
		TopicList: []string{topic},
		Key:       key,
	}

	p.logger.Debug("[async_producer] Produce input ok", logInfo)
}

func (p *asyncProducer) Close() error {
	logInfo := MainLogInfo{}

	time.Sleep(p.waitCloseTime)

	p.logger.Info("[async_producer] Close start", &logInfo)

	err := p.saramaProducer.Close()
	if err != nil {
		p.logger.Error("[async_producer] Close err", err, &logInfo)
	} else {
		p.logger.Info("[async_producer] Close ok", &logInfo)
	}

	return err
}

func (p *asyncProducer) readSuccesses() {
	for val := range p.saramaProducer.Successes() {
		msg, _ := val.Value.Encode()
		key, _ := val.Key.Encode()

		logInfo := &OffsetInfo{
			MainLogInfo: MainLogInfo{
				JsonMess:  string(msg),
				TopicList: []string{val.Topic},
				Key:       string(key),
			},
			Partition: val.Partition,
			Offset:    val.Offset,
		}

		p.logger.Debug("[async_producer] Produce ok", logInfo)
	}
}

func (p *asyncProducer) readErrors() {
	for err := range p.saramaProducer.Errors() {
		val := err.Msg
		msg, _ := val.Value.Encode()
		key, _ := val.Key.Encode()

		logInfo := &OffsetInfo{
			MainLogInfo: MainLogInfo{
				JsonMess:  string(msg),
				TopicList: []string{val.Topic},
				Key:       string(key),
			},
			Partition: val.Partition,
			Offset:    val.Offset,
		}

		p.logger.Error("[async_producer] Produce err", err, logInfo)
	}
}
