package connection

import (
	"encoding/json"
	"fmt"
	"github.com/go-dmux/core"
	sink "github.com/go-dmux/http"
	source "github.com/go-dmux/pulsar"
)

//PulsarConnConfig holds config to connect pulsar source to http sink
type PulsarConnConfig struct {
	Dmux        core.DmuxConf     `json:"dmux"`
	Source      source.PulsarConf `json:"source"`
	Sink        sink.HTTPSinkConf `json:"sink"`
	PendingAcks int
}

//PulsarConn abstracts connection
type PulsarConn struct {
	EnableDebugLog bool
	Conf           interface{}
}

//getConfiguration parses configs and returns connection config
func (c *PulsarConn) getConfiguration() *PulsarConnConfig {
	data, _ := json.Marshal(c.Conf)
	var config *PulsarConnConfig
	json.Unmarshal(data, &config)
	return config
}

//Run starts connection from source to sink
func (c *PulsarConn) Run() {
	conf := c.getConfiguration()
	fmt.Println("starting go-dmux with conf", conf)

	src := source.GetPulsarSource(conf.Source)
	tracker := source.GetCursorTracker(conf.PendingAcks, src)
	hook := source.GetPulsarHook(tracker, c.EnableDebugLog)

	snk := sink.GetHTTPSink(conf.Dmux.Size, conf.Sink)
	snk.RegisterHook(hook)
	src.RegisterHook(hook)

	h := GetKafkaMsgHasher()
	d := core.GetDistribution(conf.Dmux.DistributorType, h)

	dmux := core.GetDmux(conf.Dmux, d)
	dmux.Connect(src, snk)
	dmux.Join()
}
