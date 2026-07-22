package peer

import (
	"bytes"
	"encoding/gob"
)

func (msg *Header) Marshal() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(msg)
	return buf.Bytes()
}

func (msg *Header) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(msg)
}

func (msg *Payload) Marshal() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	_ = enc.Encode(msg)
	return buf.Bytes()
}

func (msg *Payload) Unmarshal(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(msg)
}
