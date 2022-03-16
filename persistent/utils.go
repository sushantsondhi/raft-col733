package persistent

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/sushantsondhi/raft-col733/raft"
)

func EncodeToBytes(p interface{}) ([]byte, error) {

	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeToLogEntry(s []byte) (raft.LogEntry, error) {
	entry := raft.LogEntry{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&entry)

	return entry, err
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
