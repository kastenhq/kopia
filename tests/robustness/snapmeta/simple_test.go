package snapmeta

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kopia/kopia/tests/robustness"
)

func TestSimpleWithIndex(t *testing.T) {
	simple := NewSimple()

	gotData, err := simple.Load("non-existent-key")
	if !errors.Is(err, robustness.ErrKeyNotFound) {
		t.Fatalf("Did not get expected error: %q", err)
	}

	if gotData != nil {
		t.Fatalf("Expecting nil data return from a key that does not exist")
	}

	storeKey := "key-to-store"
	data := []byte("some stored data")
	simple.Store(storeKey, data)

	gotData, err = simple.Load(storeKey)
	if err != nil {
		t.Fatalf("Error getting key: %v", err)
	}

	if bytes.Compare(gotData, data) != 0 {
		t.Fatalf("Did not get the correct data")
	}

	simple.Delete(storeKey)

	gotData, err = simple.Load(storeKey)
	if !errors.Is(err, robustness.ErrKeyNotFound) {
		t.Fatalf("Did not get expected error: %q", err)
	}

	if gotData != nil {
		t.Fatalf("Expecting nil data return from a deleted key")
	}
}
