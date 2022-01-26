package pulsar

type PulsarConf struct {
	SubscriptionName string `json:"name"`
	Url              string `json:"url"`
	Topic            string `json:"topic"`
	ReadNewest       string `json:"read_newest"`
}
