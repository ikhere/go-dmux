package pulsar

type PulsarConf struct {
	SubscriptionName string `json:"name"`
	Url              string `json:"url"`
	Topic            string `json:"topic"`
	ReadNewest       string `json:"read_newest"` // TODO: Use this
	AuthClientId     string `json:"client_id"`
	AuthClientSecret string `json:"auth_client_secret"`
	AuthIssuerURL    string `json:"auth_issuer_url"`
	AuthAudience     string `json:"auth_audience"`
}
