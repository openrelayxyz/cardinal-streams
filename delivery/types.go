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

func (b *Batch) EncodeAvro() []byte {
  data, err := avro.Marshal(batchSchema, b)
  // This shouldn't occcur unless there's a mismatch between the Batch type and the avro schema, which should get caught in testing
  if err != nil { panic(err.Error()) }
  return data
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

func (b *SubBatchRecord) EncodeAvro() []byte {
  data, err := avro.Marshal(updateSchema, b)
  // This shouldn't occcur unless there's a mismatch between the Batch type and the avro schema, which should get caught in testing
  if err != nil { panic(err.Error()) }
  return data
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


type MessageType byte

const (
  BatchType MessageType = iota  // BatchType.$Hash
  SubBatchHeaderType            // SubBatchHeaderType.$Hash.$BatchId
  SubBatchMsgType               // SubBatchMsgType.$hash.$BatchId.$Index
  BatchMsgType                  // BatchMsgType.$hash./path/
  BatchDeleteMsgType            // BatchDeleteMsgType.$hash./path/
  ReorgType                     // ReorgType.$Hash
  ReorgCompleteType             // ReorgCompleteType.$Hash
)

func (mt MessageType) GetKey(components ...[]byte) []byte {
  result := []byte{byte(mt)}
  for _, component := range components {
    result = append(result, component...)
  }
  return result
}

func AvroInt(b int) []byte {
  data, err := avro.Marshal(intSchema, b)
  // This shouldn't occcur unless there's a mismatch between the Batch type and the avro schema, which should get caught in testing
  if err != nil { panic(err.Error()) }
  return data
}

type Message interface {
  Key() []byte
  Value() []byte
}

type ResumptionMessage interface{
  Message
  Offset() int64
  Source() string
}

func UnmarshalBatch(data []byte) (*Batch, error) {
  b := &Batch{}
  if err := avro.Unmarshal(batchSchema, data, b); err != nil { return nil, err }
  return b, nil
}
