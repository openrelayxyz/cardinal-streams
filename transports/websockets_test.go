package transports

import (
	"fmt"
	"testing"
	"math/big"
	"encoding/json"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/hexutil"
)

func TestResultMessageJSON(t *testing.T) {
	x := &resultMessage{
		Type: "batch",
		Batch: &transportBatch{
			Number: hexutil.Uint64(3),
			Weight: (*hexutil.Big)(big.NewInt(3)),
			Hash: types.Hash{},
			ParentHash: types.Hash{},
			Values: make(map[string]hexutil.Bytes),
			Deletes: []string{},
			Batches: make(map[string]types.Hash),
		},
	}
	data, err := json.Marshal(x)
	if err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(string(data))
}

// type resultMessage struct {
// 	Type string `json:"type"`
// 	Batch *transportBatch `json:"batch,omitempty"`
// 	SubBatch *transportSubbatch `json:"batch,omitempty"`
// }
//
// type transportBatch struct {
// 	Number hexutil.Uint64 `json:"number"`
// 	Weight *hexutil.Big `json:"weight"`
// 	Hash types.Hash `json:"hash"`
// 	ParentHash types.Hash `json:"parent"`
// 	Values map[string]hexutil.Bytes `json:"values"`
// 	Deletes []string `json:"deletes"`
// 	Batches map[string]types.Hash `json:"batches"`
// }
