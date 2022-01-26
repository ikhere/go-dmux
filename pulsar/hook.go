package pulsar

import (
	sink "github.com/go-dmux/http"
	"log"
	"time"
)

type SourceHook interface {
	Pre(p MessageProcessor)
}

type PulsarCursorTracker interface {
	TrackMe(p MessageProcessor)
}

type CursorTracker struct {
	ch     chan MessageProcessor
	source *PulsarSource
	size   int
}

type CursorHook struct {
	cursorTracker  PulsarCursorTracker
	enableDebugLog bool
}

//Pre is invoked - before pulsar source pushes message to DMux. This implementation
//invokes CursorTracker TrackMe method here, to track Message is queued before its execution
func (h *CursorHook) Pre(p MessageProcessor) {
	h.cursorTracker.TrackMe(p)
	if h.enableDebugLog {
		msg := p.(sink.HTTPMsg)
		log.Printf("%s after pulsar source", msg.GetDebugPath())
	}
}

func (t *CursorTracker) TrackMe(msg MessageProcessor) {
	if len(t.ch) == t.size {
		log.Printf("warning: pending_acks threshold %d reached, please increase pending_acks size", t.size)
	}
	t.ch <- msg
}

//PreHTTPCall is invoked - before HttpSink execution.
func (h *CursorHook) PreHTTPCall(msg interface{}) {
	if h.enableDebugLog {
		data := msg.(sink.HTTPMsg)
		log.Printf("%s before http sink", data.GetDebugPath())
	}
}

//PostHTTPCall is invoked - after HttpSink execution. This implementation calls
//KafkaMessage MarkDone method on the data argument of Post, to mark this
//message and successfully processed.
func (h *CursorHook) PostHTTPCall(msg interface{}, success bool) {
	data := msg.(Message)
	if success {
		data.MarkDone()
	}
	if h.enableDebugLog {
		data := msg.(sink.HTTPMsg)
		log.Printf("%s after http sink, status = %t", data.GetDebugPath(), success)
	}
}

func GetCursorTracker(size int, source *PulsarSource) PulsarCursorTracker {
	t := &CursorTracker{
		ch:     make(chan MessageProcessor, size),
		source: source,
		size:   size,
	}
	go t.run()
	return t
}

func GetPulsarHook(tracker PulsarCursorTracker, enableDebugLog bool) *CursorHook {
	return &CursorHook{tracker, enableDebugLog}
}

func (t *CursorTracker) run() {
	for msg := range t.ch {
		for msg.IsProcessed() {
			time.Sleep(100 * time.Microsecond)
		}
		t.source.commitCursor(msg)
	}
}
