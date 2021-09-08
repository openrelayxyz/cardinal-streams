package delivery

import (
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  "strings"
  "regexp"
)

type batchInfo struct{
  topic string
  block types.Hash
}

type Producer struct {
  schema         []regexpPair // Map prefix -> topic
  defaultTopic   string
  pendingBatches map[types.Hash]*batchInfo // Map batchid -> batchInfo
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
  return &Producer{defaultTopic: defaultTopic, schema: s, pendingBatches: make(map[types.Hash]*batchInfo)}, nil
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

func (p *Producer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) (map[string][]Message, error) {

  b := &Batch{
    Number: number,
    Weight: weight.Bytes(),
    ParentHash: parentHash,
    Updates: make(map[string]BatchRecord),
  }
  topicMessages := make(map[string][]Message)
  topicMessages[p.defaultTopic] = []Message{}
  counts := make(map[string]int)
  // Create messages for udpates
  for k, v := range updates {
    matched := false
    for _, r := range p.schema {
      if prefix := r.regexp.FindString(k); prefix != "" {
        if _, ok := topicMessages[r.topic]; !ok { topicMessages[r.topic] = []Message{} }
        counts[prefix]++
        topicMessages[r.topic] = append(topicMessages[r.topic], &message{
          key: BatchMsgType.GetKey(hash.Bytes(), []byte(k)),
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
          key: BatchDeleteMsgType.GetKey(hash.Bytes(), []byte(k)),
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
    if _, ok := b.Updates[k]; ok { return nil, ErrPrefixConflict }
    b.Updates[k] = BatchRecord{Subbatch: h}
    bi := &batchInfo{
      topic: p.defaultTopic,
      block: hash,
    }
    for _, r := range p.schema {
      if prefix := r.regexp.FindString(k); prefix != "" {
        bi.topic = r.topic
        break
      }
    }
    p.pendingBatches[h] = bi
  }
  for k, c := range counts {
    if _, ok := b.Updates[k]; ok { return nil, ErrPrefixConflict }
    b.Updates[k] = BatchRecord{Count: c}
  }
  topicMessages[p.defaultTopic] = append(topicMessages[p.defaultTopic], &message{
      key: BatchType.GetKey(hash.Bytes()),
      value: b.EncodeAvro(),
  })
  return topicMessages, nil
}

func (p *Producer) SendBatch(batchid types.Hash, delete []string, update map[string][]byte) (map[string][]Message, error) {
  bi, ok := p.pendingBatches[batchid]
  if !ok { return nil, ErrUnknownBatch }
  topicMessages := make(map[string][]Message)
  deleteRecords := 0
  if len(delete) > 0 { deleteRecords = 1}
  topicMessages[bi.topic] = make([]Message, 1, len(update) + deleteRecords)
  counter := 0
  if deleteRecords > 0 {
    topicMessages[bi.topic] = append(topicMessages[bi.topic], &message{
      key: SubBatchMsgType.GetKey(bi.block.Bytes(), batchid.Bytes(), AvroInt(counter)),
      value: (&SubBatchRecord{Delete: delete}).EncodeAvro(),
    })
    counter++
  }
  for k, v := range update {
    topicMessages[bi.topic] = append(
      topicMessages[bi.topic],
      &message{
        key: SubBatchMsgType.GetKey(bi.block.Bytes(), batchid.Bytes(), AvroInt(counter)),
        value: (&SubBatchRecord{Updates: map[string][]byte{k: v}}).EncodeAvro(),
      },
    )
    counter++
  }
  topicMessages[bi.topic][0] = &message{key: SubBatchHeaderType.GetKey(bi.block.Bytes(), batchid.Bytes()), value: AvroInt(counter)}
  return topicMessages, nil
}


// Reorg should be called for large reorgs (> reorgThreshold). The number and
// hash should correspond to the common ancestor between the two reorged
// chains.
func (p *Producer) Reorg(number int64, hash types.Hash) Message {
  return &message{key: ReorgType.GetKey(hash.Bytes()), value: AvroInt(int(number))}
}
// ReorgDone should be called at the end of a large reorg, to indicate to the
// consumer that all messages for the reorg are available
func (p *Producer) ReorgDone(number int64, hash types.Hash) Message {
  return &message{key: ReorgCompleteType.GetKey(hash.Bytes()), value: AvroInt(int(number))}
}
