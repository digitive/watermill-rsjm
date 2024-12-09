package rsjm

import (
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
)

type JsonMarshaler struct{}

func (JsonMarshaler) Marshal(_ string, msg *message.Message) (map[string]interface{}, error) {
	if value := msg.Metadata.Get(redisstream.UUIDHeaderKey); value != "" {
		return nil, fmt.Errorf("metadata %s is reserved by watermill for message UUID", redisstream.UUIDHeaderKey)
	}

	var (
		md  []byte
		err error
	)
	if len(msg.Metadata) > 0 {
		if md, err = json.Marshal(msg.Metadata); err != nil {
			return nil, fmt.Errorf("marshal metadata fail: %w", err)
		}
	}

	return map[string]interface{}{
		redisstream.UUIDHeaderKey: msg.UUID,
		"metadata":                md,
		"payload":                 []byte(msg.Payload),
	}, nil
}

func (JsonMarshaler) Unmarshal(values map[string]interface{}) (msg *message.Message, err error) {
	msg = message.NewMessage(values[redisstream.UUIDHeaderKey].(string), []byte(values["payload"].(string)))

	md := values["metadata"]
	if md != nil {
		s := md.(string)
		if s != "" {
			metadata := make(message.Metadata)
			if err := json.Unmarshal([]byte(s), &metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata fail: %w", err)
			}
			msg.Metadata = metadata
		}
	}

	return msg, nil
}
