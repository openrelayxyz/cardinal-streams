package delivery

import (
  "bytes"
  "math/big"
  "testing"
  "github.com/hamba/avro"
  "github.com/openrelayxyz/cardinal-types"
  // "fmt"
)


func TestProducer(t *testing.T) {
  p, err := NewProducer(
    "default",
    map[string]string{
      "foo/": "foo",
      "bar/[^/]+/baz/": "bar",
    },
  )
  if err != nil { t.Errorf(err.Error()) }
  msgs := p.AddBlock(
    0,
    types.HexToHash("01"),
    types.HexToHash("00"),
    new(big.Int),
    map[string][]byte{
      "foo/something": []byte("gnihtemos/oof"),
      "bar/whatever/baz/stuff": []byte("data"),
      "default/thing": []byte("defaulttopic"),
    },
    map[string]struct{}{
      "foo/delete": struct{}{},
      "bar/delete/baz/thing": struct{}{},
      "default/delete": struct{}{},
    },
    map[string]types.Hash{
      "foo/": types.HexToHash("ff"),
    },
  )
  if l := len(msgs["foo"]); l != 2 {
    t.Errorf("Unexpected message count on topic foo. Expected 2, got %v", l)
  }
  if l := len(msgs["bar"]); l != 2 {
    t.Errorf("Unexpected message count on topic bar. Expected 2, got %v", l)
  }
  if l := len(msgs["default"]); l != 1 {
    t.Errorf("Unexpected message count on default topic. Expected 1 got %v", l)
  }
  b := Batch{}
  if err := avro.Unmarshal(batchSchema, msgs["default"][0].Value(), &b); err != nil {
    t.Errorf(err.Error())
  }
  if b.Number != 0 { t.Errorf("Unexpected batch number") }
  if !bytes.Equal(b.Weight, new(big.Int).Bytes()) { t.Errorf("Unexpected weight") }
  if b.ParentHash != types.HexToHash("00") { t.Errorf("Unexpected hash" ) }
  if l := len(b.Updates); l != 5 { t.Errorf("Unexxpecte updates length; Expected 5, got %v", l)} // 2 prefixes, 1 batch, 2 changes not in schema
  // Number int64          `avro:"num"`
  // Weight []byte          `avro:"weight"`
  // ParentHash types.Hash  `avro:"parent"`
  // Updates map[string]BatchRecord `avro:"updates"`
  // Hash types.Hash

}
