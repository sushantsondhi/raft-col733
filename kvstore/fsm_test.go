package kvstore_test

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/sushantsondhi/raft-col733/common"
	"github.com/sushantsondhi/raft-col733/kvstore"
	"testing"
)

func TestKeyValFSM_Apply(t *testing.T) {
	setMarshaller := func(key, val string) common.LogEntry {
		bytes, err := json.Marshal(kvstore.Request{
			Type: kvstore.Set,
			Key:  key,
			Val:  val,
		})
		assert.NoError(t, err)
		return common.LogEntry{
			Data: bytes,
		}
	}
	getMarshaller := func(key string) common.LogEntry {
		bytes, err := json.Marshal(kvstore.Request{
			Type: kvstore.Get,
			Key:  key,
		})
		assert.NoError(t, err)
		return common.LogEntry{
			Data: bytes,
		}
	}

	fsm := kvstore.NewKeyValFSM()
	var bytes []byte
	var err error
	// set some values in the fsm
	bytes, err = fsm.Apply(setMarshaller("a", "1"))
	assert.NoError(t, err)
	assert.Nil(t, bytes)

	bytes, err = fsm.Apply(setMarshaller("b", "1"))
	assert.NoError(t, err)
	assert.Nil(t, bytes)

	// get some values
	bytes, err = fsm.Apply(getMarshaller("a"))
	assert.NoError(t, err)
	assert.EqualValues(t, bytes, []byte("1"))

	bytes, err = fsm.Apply(getMarshaller("b"))
	assert.NoError(t, err)
	assert.EqualValues(t, bytes, []byte("1"))

	// try to get key that does not exist
	bytes, err = fsm.Apply(getMarshaller("c"))
	assert.EqualError(t, err, "key does not exist")

	// set value again
	bytes, err = fsm.Apply(setMarshaller("a", "2"))
	assert.NoError(t, err)
	assert.Nil(t, bytes)

	// get should return the new value
	bytes, err = fsm.Apply(getMarshaller("a"))
	assert.NoError(t, err)
	assert.EqualValues(t, bytes, []byte("2"))
}

// TODO: Write tests for idempotence
