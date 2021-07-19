package delivery

import (
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/hamba/avro"
  "strings"
  "regexp"
)

type Producer struct {
  schema       []regexpPair // Map prefix -> topic
  defaultTopic string
}

func NewProducer(defaultTopic string, schema map[string]string) (*Producer, error) {
  s := make([]regexpPair, len(schema))
  counter := 0
  for i, topic := range schema {
    for j := range schema {
      if i != j && strings.HasPrefix(i, j) {
        return nil, ErrPrefixOverlap
      }
    }
    exp, err := regexp.Compile(i)
    if err != nil { return nil, err }
    if !strings.HasPrefix(i, "^") {
      exp, err = regexp.Compile("^" + i)
      if err != nil { return nil, err }
    }
    s[counter] = regexpPair{topic: topic, regexp: exp}
    counter++
  }
  return &Producer{defaultTopic: defaultTopic, schema: s}, nil
}

type regexpPair struct{
  regexp *regexp.Regexp
  topic string
}

type message struct{
  key   []byte
  value []byte
}

func (m *message) Key() []byte { return m.key }
func (m *message) Value() []byte { return m.value }

func (p *Producer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) map[string][]Message {

  b := &Batch{
    Number: number,
    Weight: weight.Bytes(),
    ParentHash: parentHash,
    Updates: make(map[string]BatchRecord),
  }
  topicMessages := make(map[string][]Message)
  counts := make(map[string]int)
  // Create messages for udpates
  for k, v := range updates {
    matched := false
    for _, r := range p.schema {
      if prefix := r.regexp.FindString(k); prefix != "" {
        if _, ok := topicMessages[r.topic]; !ok { topicMessages[r.topic] = []Message{} }
        counts[prefix]++
        topicMessages[r.topic] = append(topicMessages[r.topic], &message{
          key: append(append([]byte{byte(BatchMsgType)}, hash.Bytes()...), []byte(k)...),
          value: v,
        })
        matched = true
        break
      }
    }
    if !matched {
      b.Updates[k] = BatchRecord{Value: v}
    }
  }
  // Create messages for deletes
  for k := range deletes {
    matched := false
    for _, r := range p.schema {
      if prefix := r.regexp.FindString(k); prefix != "" {
        if _, ok := topicMessages[r.topic]; !ok { topicMessages[r.topic] = []Message{} }
        counts[prefix]++
        topicMessages[r.topic] = append(topicMessages[r.topic], &message{
          key: append(append([]byte{byte(BatchDeleteMsgType)}, hash.Bytes()...), []byte(k)...),
        })
        matched = true
        break
      }
    }
    if !matched {
      b.Updates[k] = BatchRecord{Delete: true}
    }
  }
  //Create batch messages
  for k, h := range batches {
    b.Updates[k] = BatchRecord{Subbatch: h}
  }
  for k, c := range counts {
    b.Updates[k] = BatchRecord{Count: c}
  }
  data, err := avro.Marshal(batchSchema, b)
  // This shouldn't occcur unless there's a mismatch between the Batch type and the avro schema, which should get caught in testing
  if err != nil { panic(err.Error()) }
  topicMessages[p.defaultTopic] = []Message{
    &message{
      key: append([]byte{byte(BatchType)}, hash.Bytes()...),
      value: data,
    },
  }
  return topicMessages


  // type Batch struct{
  //   Number int64          `avro:"num"`
  //   Weight []byte          `avro:"weight"`
  //   ParentHash types.Hash  `avro:"parent"`
  //   Updates map[string]BatchRecord `avro:"updates"`
  //   Hash types.Hash
  // }
}
