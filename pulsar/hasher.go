package pulsar

import (
	"github.com/go-dmux/core"
	"hash/fnv"
)

type MessageHasher struct {
}

func GetMessageHasher() core.Hasher {
	return new(MessageHasher)
}

func (m *MessageHasher) ComputeHash(data interface{}) int {
	processor := data.(MessageProcessor)
	hash32 := fnv.New32a()
	hash32.Write([]byte(processor.GetRawMsg().Key()))
	return int(hash32.Sum32())
}
