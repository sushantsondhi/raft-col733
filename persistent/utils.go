package persistent

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/sushantsondhi/raft-col733/common"
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

func DecodeToLogEntry(s []byte) (common.LogEntry, error) {
	entry := common.LogEntry{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&entry)

	return entry, err
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func int64ToBytes(i int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}
