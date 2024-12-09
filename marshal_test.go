package rsjm

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func TestDefaultMarshallerUnmarshaller_MarshalUnmarshal(t *testing.T) {
	require := require.New(t)
	m := JsonMarshaler{}

	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(err)

	consumerMessage, err := producerToConsumerMessage(marshaled)
	require.NoError(err)
	unmarshaledMsg, err := m.Unmarshal(consumerMessage)
	require.NoError(err)

	require.True(msg.Equals(unmarshaledMsg))
}

func producerToConsumerMessage(producerMessage map[string]interface{}) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	for k, v := range producerMessage {
		if b, ok := v.([]byte); ok {
			res[k] = string(b)
		} else {
			res[k] = v
		}
	}
	return res, nil
}
