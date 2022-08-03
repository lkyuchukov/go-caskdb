package gocaskdb

import (
	"bytes"
	"encoding/binary"
)

type Encoder struct{}

// EncodeKV encodes a timestamp, key and value into a byte array.
func (e *Encoder) EncodeKV(ts int, k string, v string) (int, []byte) {
	buf := new(bytes.Buffer)

	e.encodeHeaderField(ts, buf)
	e.encodeHeaderField(len(k), buf)
	e.encodeHeaderField(len(v), buf)

	keyBytes := []byte(k)
	buf.Write(keyBytes)

	valBytes := []byte(v)
	buf.Write(valBytes)

	return HeaderSize + len(keyBytes) + len(valBytes), buf.Bytes()
}

// DecodeKV decodes the byte array into a KeyValuePair.
func (e *Encoder) DecodeKV(b []byte) KeyValuePair {
	ts, keySize, _ := e.DecodeHeader(b)

	key := string(b[HeaderSize : HeaderSize+keySize])
	value := string(b[HeaderSize+keySize:])

	kv := KeyValuePair{timestamp: int(ts), key: key, value: value}
	return kv
}

// DecodeHeader decodes a byte array into timestamp, key size and value size.
func (e *Encoder) DecodeHeader(b []byte) (uint32, uint32, uint32) {
	ts := binary.LittleEndian.Uint32(b[:4])
	keySize := binary.LittleEndian.Uint32(b[4:8])
	valueSize := binary.LittleEndian.Uint32(b[8:])
	return ts, keySize, valueSize
}

func (e *Encoder) encodeHeaderField(field int, buf *bytes.Buffer) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(field))
	buf.Write(b)
}
