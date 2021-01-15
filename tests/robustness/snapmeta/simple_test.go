package snapmeta

import (
	"bytes"
	"sort"
	"testing"
)

func TestSimpleWithIndex(t *testing.T) {
	type opType int

	const (
		storeOpType opType = iota
		deleteOpType
	)

	type storeOp struct {
		op    opType
		key   string
		value []byte
		idxOp map[string]IndexOperation
	}

	type expResult struct {
		expErr   bool
		expValue []byte
	}

	for _, tc := range []struct {
		name            string
		storeOperations []storeOp
		expectedKVs     map[string]expResult // List of expected key-values
		expectedIdxKeys map[string][]string  // List of expected keys in the given index
	}{
		{
			name: "Test Store Operation",
			storeOperations: []storeOp{
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-value"),
					idxOp: nil,
				},
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-different-value"),
					idxOp: nil,
				},
			},
			expectedKVs: map[string]expResult{
				"some-key": {
					expErr:   false,
					expValue: []byte("some-different-value"),
				},
			},
		},
		{
			name: "Test Delete Operation",
			storeOperations: []storeOp{
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-value"),
					idxOp: nil,
				},
				{
					op:    deleteOpType,
					key:   "some-key",
					value: []byte("some-different-value"),
					idxOp: nil,
				},
			},
			expectedKVs: map[string]expResult{
				"some-key": {
					expErr:   true,
					expValue: []byte{},
				},
			},
		},

		{
			name: "Test Store Operation + AddtoIndex",
			storeOperations: []storeOp{
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-value"),
					idxOp: map[string]IndexOperation{
						"some-index-key": AddToIndexOperation,
					},
				},
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-different-value"),
					idxOp: nil,
				},
			},
			expectedKVs: map[string]expResult{
				"some-key": {
					expErr:   false,
					expValue: []byte("some-different-value"),
				},
			},
			expectedIdxKeys: map[string][]string{
				"some-index-key": {"some-key"},
			},
		},

		{
			name: "Test Store Operation + DeleteFromIndex",
			storeOperations: []storeOp{
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-value"),
					idxOp: map[string]IndexOperation{
						"some-index-key":      AddToIndexOperation,
						"some-more-index-key": AddToIndexOperation,
					},
				},
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-different-value"),
					idxOp: map[string]IndexOperation{
						"some-index-key": RemoveFromIndexOperation,
					},
				},
			},
			expectedKVs: map[string]expResult{
				"some-key": {
					expErr:   false,
					expValue: []byte("some-different-value"),
				},
			},
			expectedIdxKeys: map[string][]string{
				"some-index-key":      {},
				"some-more-index-key": {"some-key"},
			},
		},

		{
			name: "Test Delete Operation + DeleteFromIndex",
			storeOperations: []storeOp{
				{
					op:    storeOpType,
					key:   "some-key",
					value: []byte("some-value"),
					idxOp: map[string]IndexOperation{
						"some-index-key": AddToIndexOperation,
					},
				},
				{
					op:    deleteOpType,
					key:   "some-key",
					value: []byte("some-different-value"),
					idxOp: map[string]IndexOperation{
						"some-index-key": RemoveFromIndexOperation,
					},
				},
			},
			expectedKVs: map[string]expResult{
				"some-key": {
					expErr:   true,
					expValue: []byte{},
				},
			},

			expectedIdxKeys: map[string][]string{
				"some-index-key": {},
			},
		},
	} {
		t.Log(tc.name)

		simple := NewSimple()

		for _, op := range tc.storeOperations {
			switch op.op {
			case storeOpType:
				simple.Store(op.key, op.value, op.idxOp)
			case deleteOpType:
				simple.Delete(op.key, op.idxOp)
			}
		}

		// Check keys present and expected value
		for wantK, expResult := range tc.expectedKVs {
			gotV, err := simple.Load(wantK)
			if expResult.expErr != (err != nil) {
				t.Fatalf("expected %v error but got %v", expResult.expErr, err)
			}

			if bytes.Compare(expResult.expValue, gotV) != 0 { // nolint:gosimple
				t.Fatalf("expected %v key but got %v", wantK, gotV)
			}
		}

		// Check indices contain expected keys
		for idxName, wantKeys := range tc.expectedIdxKeys {
			gotKeys := simple.GetKeys(idxName)
			if !compareSlices(gotKeys, wantKeys) {
				t.Fatalf("expected %v keys but got %v", wantKeys, gotKeys)
			}
		}
	}
}

func compareSlices(slice1, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	sort.Strings(slice1)
	sort.Strings(slice2)

	for i, v := range slice1 {
		if v != slice2[i] {
			return false
		}
	}

	return true
}
