package kafka

type LogInfo struct {
	Partition int32    `json:"partition"`
	Offset    int64    `json:"offset"`
	JsonMess  []byte   `json:"jsonMess,omitempty"`
	TopicList []string `json:"topicList,omitempty"`
	HostList  []string `json:"hostList,omitempty"`
	Key       string   `json:"key,omitempty"`
	GroupId   string   `json:"groupId,omitempty"`
}

type Logger interface {
	Fatal(msg string, err error, info LogInfo)
	Debug(msg string, info LogInfo)
	Info(msg string, info LogInfo)
	Error(msg string, err error, info LogInfo)
}
