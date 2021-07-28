package delivery

import (
  "regexp"
  "fmt"
  "testing"
  "math/big"
  "math/rand"
  "github.com/openrelayxyz/cardinal-types"
  "runtime"
  "time"
  // log "github.com/inconshreveable/log15"
)

var (
  bigOne = big.NewInt(1)
)


// AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash)
// SendBatch(batchid types.Hash, delete []string, update map[string][]byte) (map[string][]Message, error)

type testSubBatch struct {
  id     types.Hash
  delete []string
  update map[string][]byte
}

type testUpdate struct{
  number     int64
  hash       types.Hash
  parentHash types.Hash
  weight     *big.Int
  updates     map[string][]byte
  deletes     map[string]struct{}
  batches     map[string]testSubBatch
}

func (tu *testUpdate) batchIds() map[string]types.Hash {
  m := make(map[string]types.Hash)
  for path, sb := range tu.batches {
    m[path] = sb.id
  }
  return m
}

func (tu *testUpdate) Messages(p *Producer) ([]ResumptionMessage, error) {
  msgs, err := p.AddBlock(tu.number, tu.hash, tu.parentHash, tu.weight, tu.updates, tu.deletes, tu.batchIds())
  if err != nil { return nil, err }
  result := toTestResumptionMessage(msgs)
  for _, b := range tu.batches {
    bmsgs, err := p.SendBatch(b.id, b.delete, b.update,)
    if err != nil { return nil, err }
    result = append(result, toTestResumptionMessage(bmsgs)...)
  }
  return result, nil
}

func getTestBlock(blockNo, nonce int64, parent *testUpdate) *testUpdate {
  tu := &testUpdate{
    number: blockNo,
    hash: types.BytesToHash([]byte(fmt.Sprintf("%v%v", nonce, blockNo))),
    updates: make(map[string][]byte),
    deletes: make(map[string]struct{}),
    batches: make(map[string]testSubBatch),
  }
  if parent != nil {
    tu.parentHash = parent.hash
    tu.weight = new(big.Int).Add(parent.weight, bigOne)
  } else {
    tu.parentHash = types.Hash{}
    tu.weight = new(big.Int)
  }
  tu.updates[fmt.Sprintf("foo/%v/item", blockNo)] = []byte(fmt.Sprintf("%v", nonce))
  tu.deletes[fmt.Sprintf("foo/%v/gone", blockNo)] = struct{}{}
  tu.batches["state/"] = testSubBatch{
    id: types.BytesToHash([]byte(fmt.Sprintf("batch%v%v", blockNo, nonce))),
    delete: []string{fmt.Sprintf("state/%v/whatever", nonce)},
    update: map[string][]byte{
      fmt.Sprintf("state/%v/addition", blockNo): []byte(fmt.Sprintf("%v", nonce)),
    },
  }
  return tu
}

func TestOrdering(t *testing.T) {
  p, err := NewProducer(
    "default",
    map[string]string{
      "foo/": "foo",
      "bar/[^/]+/baz/": "bar",
      "state/": "state",
    },
  )

  a := getTestBlock(0, 0, nil)
  b := getTestBlock(1, 0, a)
  c := getTestBlock(2, 0, b)
  if err != nil { t.Fatalf(err.Error()) }
  messages, err := b.Messages(p)
  if err != nil { t.Fatalf(err.Error()) }
  cmessages, err := c.Messages(p)
  if err != nil { t.Fatalf(err.Error()) }
  messages = append(messages, cmessages...)
  rand.Seed(time.Now().UnixNano())
  rand.Shuffle(len(messages), func(i, j int) { messages[i], messages[j] = messages[j], messages[i] })
  omp, err := NewOrderedMessageProcessor(a.number, a.hash, a.weight, 128, []*regexp.Regexp{regexp.MustCompile(".")})
  defer omp.Close()
  ch := make(chan *ChainUpdate, 5)
  sub := omp.Subscribe(ch)
  defer sub.Unsubscribe()
  for _, msg := range messages {
    if err := omp.ProcessMessage(msg); err != nil {
      t.Errorf(err.Error())
    }
  }
  runtime.Gosched()
  done := false
  added := []*PendingBatch{}
  removed := []*PendingBatch{}
  for !done {
    select {
    case update := <- ch:
      added = append(added, update.added...)
      removed = append(removed, update.removed...)
    case <- time.After(250 * time.Millisecond):
      done = true
    }
  }
  if len(added) != 2 {
    t.Errorf("Expected 2 added items, got %v", len(added))
  }
  if len(removed) != 0 {
    t.Errorf("Expected 0 removed items, got %v", len(removed))
  }
}
