package persistent_test

import (
	"github.com/sushantsondhi/raft-col733/persistent"
	"testing"
)

func TestPStore_Create(t *testing.T) {
	_, err := persistent.NewPStore("state.db")

	if err != nil {
		t.Error("db creation failed", err)
	}
}

func TestPStore_Set(t *testing.T) {

	store, err := persistent.NewPStore("state.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

	err = store.Set([]byte("key1"), []byte("val"))

	if err != nil {
		t.Error("set key1 failed", err)
	}

	err = store.Set([]byte("key2"), []byte("val"))

	if err != nil {
		t.Error("set key2 failed", err)
	}

	err = store.Set([]byte("key1"), []byte("new-val"))

	if err != nil {
		t.Error("update key1 failed", err)
	}

}

func TestPStore_Get(t *testing.T) {

	store, err := persistent.NewPStore("state.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

	err = store.Set([]byte("key1"), []byte("val"))

	if err != nil {
		t.Error("set key1 failed", err)
	}

	err = store.Set([]byte("key1"), []byte("new-val"))

	if err != nil {
		t.Error("update key1 failed", err)
	}

	val, err := store.Get([]byte("key1"))

	if err != nil {
		t.Error("get key 1 failed", err)
	}

	if string(val) != ("new-val") {
		t.Error("get returned incorrect value")
	}

	val, err = store.Get([]byte("key2"))

	if err == nil {
		t.Error("got value for invalid key", err)
	}

}

func TestPStore_GetDefault(t *testing.T) {

	store, err := persistent.NewPStore("state.db")

	if err != nil {
		t.Error("db creation failed", err)
	}

	err = store.Set([]byte("key1"), []byte("val"))

	if err != nil {
		t.Error("set key1 failed", err)
	}

	err = store.Set([]byte("key1"), []byte("new-val"))

	if err != nil {
		t.Error("update key1 failed", err)
	}

	val, err := store.GetDefault([]byte("key1"), []byte("new-val2"))

	if err != nil {
		t.Error("get key 1 failed", err)
	}
	if string(val) != ("new-val") {
		t.Error("get returned incorrect value")
	}

	val, err = store.GetDefault([]byte("key2"), []byte("default-val"))

	if err != nil {
		t.Error("got error, expected default value", err)
	}
	if string(val) != ("default-val") {
		t.Error("get returned incorrect value")
	}

}
