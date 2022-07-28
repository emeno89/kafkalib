package kafka

import (
	"github.com/google/uuid"
	"time"
)

type Message struct {
	Id        string `json:"id"`
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
	Data      string `json:"data"`
	SenderId  string `json:"senderId"`
}

func MakeMessage(name string, data string) Message {
	return Message{
		Id:        uuid.New().String(),
		Name:      name,
		Timestamp: time.Now().UnixNano() / 1000,
		Data:      data,
	}
}
