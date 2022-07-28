package kafka

type LogInfo struct {
	Partition int32
	Offset    int64
	JsonMess  []byte
	TopicList []string
	HostList  []string
	Key       string
	GroupId   string
}

type Logger interface {
	Fatal(msg string, err error, info LogInfo)
	Debug(msg string, info LogInfo)
	Info(msg string, info LogInfo)
	Error(msg string, err error, info LogInfo)
}
