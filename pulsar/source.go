package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

type PulsarSource struct {
	conf     PulsarConf
	client   pulsar.Client
	hook     SourceHook
	consumer pulsar.Consumer
}

func (p *PulsarSource) RegisterHook(hook SourceHook) {
	p.hook = hook
}

func GetPulsarSource(conf PulsarConf) *PulsarSource {
	return &PulsarSource{conf: conf}
}

//Generate is Source method implementation, which connects to Pulsar and pushes
//PulsarMessage into the channel
func (p *PulsarSource) Generate(out chan<- interface{}) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: p.conf.Url})
	if err != nil {
		panic(err)
	}

	channel := make(chan pulsar.ConsumerMessage, 100)

	options := pulsar.ConsumerOptions{
		Topic:            p.conf.Topic,
		SubscriptionName: p.conf.SubscriptionName,
		Type:             pulsar.Exclusive,
	}

	options.MessageChannel = channel
	consumer, err := client.Subscribe(options)
	if err != nil {
		client.Close()
		panic(err)
	}

	p.client = client
	p.consumer = consumer

	// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
	// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
	// shared across multiple consumers as well
	for cm := range channel {
		processor := getMessageProcessor(&cm)
		if p.hook != nil {
			p.hook.Pre(processor)
		}
		out <- processor
	}
}

//Stop method implements Source interface stop method, to Stop the KafkaConsumer
func (p *PulsarSource) Stop() {
	p.consumer.Close()
	p.client.Close()
}

func (p *PulsarSource) commitCursor(data MessageProcessor) {
	p.consumer.Ack(data.GetRawMsg())
}
