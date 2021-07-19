package delivery

import (
  "github.com/openrelayxyz/cardinal-types"
  "github.com/hamba/avro"
)


type Batch struct{
  Number int64          `avro:"num"`
  Weight []byte          `avro:"weight"`
  ParentHash types.Hash  `avro:"parent"`
  Updates map[string]BatchRecord `avro:"updates"`
  Hash types.Hash
}

type BatchRecord struct{
  Value []byte        `avro:"value"`
  Count int           `avro:"count"`
  Subbatch types.Hash `avro:"subbatch"`
  Delete bool         `avro:"delete"`
}

type SubBatchRecord struct{
  Delete []string           `avro:"delete"`
  Updates map[string][]byte `avro:"updates"`
}


var (
  batchSchema = avro.MustParse(`{
    "type": "record",
    "name": "batch",
    "avro.codec": "snappy",
    "namespace": "cloud.rivet.cardinal",
    "fields": [
      {"name": "num", "type": "long"},
      {"name": "weight", "type": "bytes"},
      {"name": "parent", "type": {"name": "par", "type": "fixed", "size": 32}},
      {
        "name": "updates",
        "type": {"name": "upd", "type": "map", "values": {
          "type": "record",
          "name": "update",
          "fields": [
            {"name": "value", "type": "bytes"},
            {"name": "count", "type": "int"},
            {"name": "delete", "type": "boolean"},
            {"name": "subbatch", "type": {"name": "subb", "type": "fixed", "size": 32}}
          ]
        }}
      }
    ]
  }`)
  updateSchema = avro.MustParse(`{
    "type": "record",
    "name": "update",
    "fields": [
      {"name": "delete", "type": {"name": "del", "type": "array", "items": "string"}},
      {"name": "updates", "type": {"name": "upd", "type": "map", "values": "bytes"}}
    ]
  }`)
  intSchema = avro.MustParse("int")
)
