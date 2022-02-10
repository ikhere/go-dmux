package pulsar

import (
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	sink "github.com/go-dmux/http"
	"strconv"
	"strings"
)

type Message struct {
	Msg       *pulsar.ConsumerMessage
	Processed bool
	Sidelined bool
}

type MessageProcessor interface {
	MarkDone()
	GetRawMsg() *pulsar.ConsumerMessage
	IsProcessed() bool
}

func (m *Message) MarkDone() {
	m.Processed = true
}

func (m *Message) GetRawMsg() *pulsar.ConsumerMessage {
	return m.Msg
}

func (m *Message) IsProcessed() bool {
	return m.Processed
}

// GetPayload implements HTTPMsg interface
func (m *Message) GetPayload() []byte {
	return m.Msg.Payload()
}

// GetDebugPath implements HTTPMsg interface
func (m *Message) GetDebugPath() string {
	return fmt.Sprintf("/%s/%s/%s/%s/%s/%s",
		m.Msg.Topic(), m.Msg.Key(),
		strconv.FormatInt(m.Msg.ID().LedgerID(), 10),
		strconv.FormatInt(m.Msg.ID().EntryID(), 10),
		strconv.FormatInt(int64(m.Msg.ID().BatchIdx()), 10),
		strconv.FormatInt(int64(m.Msg.ID().PartitionIdx()), 10))
}

// GetURL implements HTTPMsg interface
func (m *Message) GetURL(endpoint string) string {
	return endpoint + m.GetDebugPath()
}

// GetHeaders implements HTTPMsg interface
func (m *Message) GetHeaders(conf sink.HTTPSinkConf) map[string]string {
	header := make(map[string]string)
	for _, val := range conf.Headers {
		header[val["name"]] = val["value"]
	}
	header["Content-Type"] = "application/json" //force json for foxtrot

	return header
}

//CustomURLKey  place holder name, which will be replaced by kafka key
const CustomURLKey = "__KEY_NAME__"

// BatchURL implements HTTPMsg interface
func (m *Message) BatchURL(msgs []interface{}, endpoint string, version int) string {
	url := strings.Replace(endpoint, CustomURLKey, m.Msg.Key(), 1)
	url = url + "/bulk"

	var builder strings.Builder
	topic := ""
	for i, msg := range msgs {
		msg := msg.(*Message)
		if i == 0 {
			_topic := strings.Split(msg.Msg.Topic(), "/")
			topic = _topic[len(_topic)-1]
		} else {
			builder.WriteString("~")
		}
		builder.WriteString(strconv.FormatInt(int64(msg.Msg.ID().PartitionIdx()), 10))
		builder.WriteString(",")
		builder.WriteString(fmt.Sprintf("%s.%s.%s",
			strconv.FormatInt(msg.Msg.ID().EntryID(), 10),
			strconv.FormatInt(msg.Msg.ID().LedgerID(), 10),
			strconv.FormatInt(int64(msg.Msg.ID().BatchIdx()), 10)))
	}
	return url + "?topic=" + topic + "&batch=" + builder.String()
}

// BatchPayload implements HTTPMsg interface
func (m *Message) BatchPayload(msgs []interface{}, version int) []byte {
	payload := make([]interface{}, len(msgs))
	for i := 0; i < len(msgs); i++ {
		msg := msgs[i].(*Message)
		data := msg.GetPayload()
		var obj interface{}
		err := json.Unmarshal(data, &obj)
		if err != nil {
			panic("failed to unmarshal data in batch payload construction")
		}
		payload[i] = obj
	}

	output, err := json.Marshal(payload)
	if err != nil {
		panic("failed to marshal batch data into payload construction")
	}
	return output
}

func getMessageProcessor(msg *pulsar.ConsumerMessage) MessageProcessor {
	return &Message{Msg: msg, Processed: false}
}
