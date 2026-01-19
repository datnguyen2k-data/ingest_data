package avro

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/linkedin/goavro/v2"
)

// Encoder wraps goavro codec for thread-safe encoding
type Encoder struct {
	codec *goavro.Codec
	mu    sync.Mutex
}

// NewEncoder creates a new encoder from an Avro schema string
func NewEncoder(schema string) (*Encoder, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro codec: %w", err)
	}
	return &Encoder{
		codec: codec,
	}, nil
}

// EncodeJSON converts a JSON byte slice to Avro binary format
func (e *Encoder) EncodeJSON(jsonData []byte) ([]byte, error) {
	var native interface{}
	if err := json.Unmarshal(jsonData, &native); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	// Native format for goavro is map[string]interface{}
	// Validate that unmarshaled data is compatible
	if _, ok := native.(map[string]interface{}); !ok {
		// If it's not a map (e.g. array or primitive), we might need to wrap it or fail
		// For typical Avro record, it expects a map
		return nil, fmt.Errorf("json data must be an object/record to match avro schema")
	}

	binary, err := e.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to avro binary: %w", err)
	}

	return binary, nil
}

// EncodeNative converts a Go native map to Avro binary format
func (e *Encoder) EncodeNative(native interface{}) ([]byte, error) {
	binary, err := e.codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode to avro binary: %w", err)
	}
	return binary, nil
}
