package delivery

import (
  "testing"
  "github.com/hamba/avro"
  "github.com/openrelayxyz/cardinal-types"
  // "fmt"
)


func TestSerializer(t *testing.T) {
  b := &Batch{
    Number: 0,
    Weight: []byte{0},
    ParentHash: types.HexToHash("01"),
    Updates: make(map[string]BatchRecord),
  }
  b.Updates["/foo"] = BatchRecord{Value: []byte("bar")}
  b.Updates["/baz"] = BatchRecord{Count: 1}
  b.Updates["/batch"] = BatchRecord{Subbatch: types.HexToHash("02")}
  data, err := avro.Marshal(batchSchema, b)
  if err != nil { t.Errorf(err.Error()) }

  nb := &Batch{}
  if err := avro.Unmarshal(batchSchema, data, nb); err != nil { t.Errorf(err.Error()) }

  if nb.Number != b.Number { t.Errorf("Unexpected number in decoded obect")}
  if len(nb.Weight) != len(b.Weight) { t.Errorf("Unexpected weight in decoded obect")}
  if nb.ParentHash != b.ParentHash { t.Errorf("Unexpected ParentHash in decoded obect")}
  if len(nb.Updates) != len(b.Updates) { t.Errorf("Unexpected update size in decoded obect")}
  if nb.Updates["/baz"].Count != b.Updates["/baz"].Count { t.Errorf("Unexpected baz count in decoded object")}

  data, err = avro.Marshal(updateSchema, &SubBatchRecord{Delete: []string{"/foo"}})
  if err != nil { t.Errorf(err.Error()) }

  u := SubBatchRecord{}
  if err := avro.Unmarshal(updateSchema, data, &u); err != nil { t.Errorf(err.Error()) }
  if u.Delete[0] != "/foo" { t.Errorf("Unexpected unmarshalled delete")}
}
