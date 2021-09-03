package transports

import (
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
)

type Producer interface {
  AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error
  SendBatch(batchid types.Hash, delete []string, update map[string][]byte) error
  Reorg(number int64, hash types.Hash) (func(), error)
}
