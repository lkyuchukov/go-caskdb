package gocaskdb

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
)

func TestEncodeKV(t *testing.T) {
	ts := int(time.Now().Unix())
	enc := Encoder{}
	size, _ := enc.EncodeKV(ts, "foo", "bar")
	assert.Equal(t, 18, size)
}

func TestDecodeKV(t *testing.T) {
	// this is the result of encoding the timestamp 1659197752, the key "foo", and the value "bar"
	b := []byte{56, 89, 229, 98, 3, 0, 0, 0, 3, 0, 0, 0, 102, 111, 111, 98, 97, 114}
	enc := Encoder{}
	kv := enc.DecodeKV(b)
	assert.Equal(t, "foo", kv.key)
	assert.Equal(t, "bar", kv.value)
}

func TestDecodeHeader(t *testing.T) {
	// this is the result of encoding the timestamp 1659197752, the key "foo", and the value "bar"
	b := []byte{56, 89, 229, 98, 3, 0, 0, 0, 3, 0, 0, 0, 102, 111, 111, 98, 97, 114}
	enc := Encoder{}
	ts, keySize, valueSize := enc.DecodeHeader(b)

	assert.Equal(t, uint32(1659197752), ts)
	assert.Equal(t, uint32(3), keySize)
	assert.Equal(t, uint32(3), valueSize)
}