package kafka

import "encoding/json"

type LogInfo interface {
	String() string
}

type MainLogInfo struct {
	JsonMess  string   `json:"jsonMess,omitempty"`
	TopicList []string `json:"topicList,omitempty"`
	HostList  []string `json:"hostList,omitempty"`
	Key       string   `json:"key,omitempty"`
	GroupId   string   `json:"groupId,omitempty"`
}

func (s *MainLogInfo) String() string {
	res, _ := json.Marshal(s)

	return string(res)
}

type OffsetInfo struct {
	MainLogInfo
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (s *OffsetInfo) String() string {
	res, _ := json.Marshal(s)

	return string(res)
}

type Logger interface {
	Fatal(msg string, err error, info LogInfo)
	Debug(msg string, info LogInfo)
	Info(msg string, info LogInfo)
	Error(msg string, err error, info LogInfo)
}
