package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

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

func getMessageProcessor(msg *pulsar.ConsumerMessage) MessageProcessor {
	return &Message{Msg: msg, Processed: false}
}
