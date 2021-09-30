package delivery

import (
  "time"
  "testing"
  "math/big"
  "math/rand"
  "github.com/openrelayxyz/cardinal-types"
  // "strings"
  "regexp"
  "runtime"
)

type testResumptionMessage struct{
  Message
  offset int64
  source string
}

func (r *testResumptionMessage) Offset() int64 { return r.offset }
func (r *testResumptionMessage) Source() string { return r.source }
func (r *testResumptionMessage) Time() time.Time { return time.Now() }



func toTestResumptionMessage(inputs... map[string][]Message) []ResumptionMessage {
  offsets := make(map[string]int64)
  result := []ResumptionMessage{}
  for _, input := range inputs {
    for k, msgs := range input {
      for _, msg := range msgs {
        result = append(result, &testResumptionMessage{Message: msg, offset: offsets[k], source: k})
        offsets[k]++
      }
    }
  }
  return result
}


func TestConsumer(t *testing.T) {
  p, err := NewProducer(
    "default",
    map[string]string{
      "foo/": "foo",
      "bar/[^/]+/baz/": "bar",
      "state/thing": "state",
    },
  )
  if err != nil { t.Errorf(err.Error()) }
  mp := NewMessageProcessor(0, 128, []*regexp.Regexp{regexp.MustCompile(".*")})
  msgs, err := p.AddBlock(
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
      "state/": types.HexToHash("ff"),
    },
  )
  if err != nil { t.Fatalf(err.Error()) }
  ch := make(chan *PendingBatch, 5)
  sub := mp.Subscribe(ch)
  defer sub.Unsubscribe()
  msgList := toTestResumptionMessage(msgs)
  for _, msg := range msgList {
    if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    t.Errorf("Expected nothing on channel yet, got %v, %v, %v", v.pendingBatches, v.prefixes, v.batches)
  default:
  }

  msgs, err = p.SendBatch(types.HexToHash("ff"), []string{"whatever/", "other/"}, map[string][]byte{"state/thing": []byte("thing!")})
  if err != nil { t.Fatalf(err.Error()) }
  msgList = toTestResumptionMessage(msgs)
  for _, msg := range msgList {
    if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    if v.Number != 0 { t.Errorf("Unexpected batch number") }
    if v.Weight.Cmp(new(big.Int)) != 0 { t.Errorf("Unexpected weight") }
    if v.ParentHash != types.HexToHash("00") { t.Errorf("Unexpected hash" ) }
    if l := len(v.Values); l != 4 { t.Errorf("Unexpected updates length; Expected 4, got %v", l)} // 2 prefixes, 1 batch, 2 changes not in schema
    if l := len(v.Deletes); l != 5 { t.Errorf("Unexpected deletes length; Expected 5, got %v", v.Deletes)} // 2 prefixes, 1 batch, 2 changes not in schema
  default:
    t.Errorf("Expected item on channel, nothing yet")
  }
  select {
  case v := <-ch:
    t.Errorf("Unexpected item on channel: %v", v)
  default:
  }
}

func getTestMessages(t *testing.T, blockCount int) []ResumptionMessage {
  p, err := NewProducer(
    "default",
    map[string]string{
      "foo/": "foo",
      "bar/[^/]+/baz/": "bar",
      "state/thing": "state",
    },
  )
  if err != nil { t.Errorf(err.Error()) }
  msgs := []map[string][]Message{}
  for i := 1; i <= blockCount ; i++ {
    blockHash := types.BytesToHash([]byte{byte(i)})
    parentHash := types.BytesToHash([]byte{byte(i-1)})
    batchid := types.BytesToHash([]byte{255, byte(i-1)})
    m, err := p.AddBlock(
      int64(i),
      blockHash,
      parentHash,
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
        "state/": batchid,
      },
    )
    if err != nil { t.Fatalf(err.Error()) }
    msgs = append(msgs, m)
    m, err = p.SendBatch(batchid, []string{"whatever/", "other/"}, map[string][]byte{"state/thing": []byte("thing!")})
    if err != nil { t.Fatalf(err.Error()) }
    msgs = append(msgs, m)
  }
  return toTestResumptionMessage(msgs...)
}

func TestShuffled(t *testing.T) {
  mp := NewMessageProcessor(0, 128, []*regexp.Regexp{regexp.MustCompile(".*")})
  ch := make(chan *PendingBatch, 5)
  sub := mp.Subscribe(ch)
  defer sub.Unsubscribe()
  msgList := getTestMessages(t, 1)
  rand.Seed(time.Now().UnixNano())
  rand.Shuffle(len(msgList), func(i, j int) { msgList[i], msgList[j] = msgList[j], msgList[i] })
  for _, msg := range msgList[:] {
    if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    if v.Number != 1 { t.Errorf("Unexpected batch number") }
    if v.Weight.Cmp(new(big.Int)) != 0 { t.Errorf("Unexpected weight") }
    if v.ParentHash != types.HexToHash("00") { t.Errorf("Unexpected hash" ) }
    if l := len(v.Values); l != 4 { t.Errorf("Unexpected updates length; Expected 4, got %v", l)} // 2 prefixes, 1 batch, 2 changes not in schema
    if l := len(v.Deletes); l != 5 { t.Errorf("Unexpected deletes length; Expected 5, got %v", v.Deletes)} // 2 prefixes, 1 batch, 2 changes not in schema
  default:
    t.Errorf("Expected item on channel, nothing yet (%v) - %v", mp.pendingBatches[types.HexToHash("01")].whyNotReady(), msgList[len(msgList) - 1])
  }
  select {
  case v := <-ch:
    t.Errorf("Unexpected item on channel: %v", v)
  default:
  }
}

func TestShuffledDups(t *testing.T) {
  mp := NewMessageProcessor(0, 128, []*regexp.Regexp{regexp.MustCompile(".*")})
  ch := make(chan *PendingBatch, 5)
  sub := mp.Subscribe(ch)
  defer sub.Unsubscribe()
  msgList := getTestMessages(t, 1)
  rand.Seed(time.Now().UnixNano())
  rand.Shuffle(len(msgList), func(i, j int) { msgList[i], msgList[j] = msgList[j], msgList[i] })
  msgList = append(msgList, msgList[:len(msgList) / 4]...)
  rand.Shuffle(len(msgList), func(i, j int) { msgList[i], msgList[j] = msgList[j], msgList[i] })
  for _, msg := range msgList[:] {
    if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    if v.Number != 1 { t.Errorf("Unexpected batch number") }
    if v.Weight.Cmp(new(big.Int)) != 0 { t.Errorf("Unexpected weight") }
    if v.ParentHash != types.HexToHash("00") { t.Errorf("Unexpected hash" ) }
    if l := len(v.Values); l != 4 { t.Errorf("Unexpected updates length; Expected 4, got %v", l)} // 2 prefixes, 1 batch, 2 changes not in schema
    if l := len(v.Deletes); l != 5 { t.Errorf("Unexpected deletes length; Expected 5, got %v", v.Deletes)} // 2 prefixes, 1 batch, 2 changes not in schema
  default:
    t.Errorf("Expected item on channel, nothing yet (%v) - %v", mp.pendingBatches[types.HexToHash("01")].whyNotReady(), msgList[len(msgList) - 1])
  }
  select {
  case v := <-ch:
    t.Errorf("Unexpected item on channel: %v", v)
  default:
  }
}

func TestShuffledDupsMultiBlock(t *testing.T) {
  mp := NewMessageProcessor(0, 128, []*regexp.Regexp{regexp.MustCompile(".*")})
  ch := make(chan *PendingBatch, 5)
  sub := mp.Subscribe(ch)
  defer sub.Unsubscribe()
  msgList := getTestMessages(t, 2)
  rand.Seed(time.Now().UnixNano())
  rand.Shuffle(len(msgList), func(i, j int) { msgList[i], msgList[j] = msgList[j], msgList[i] })
  msgList = append(msgList, msgList[:len(msgList) / 4]...)
  rand.Shuffle(len(msgList), func(i, j int) { msgList[i], msgList[j] = msgList[j], msgList[i] })
  for _, msg := range msgList[:] {
    if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
  }
  runtime.Gosched()
  select {
  case <-ch:
  default:
    t.Errorf("Expected item on channel, nothing yet (%v) - %v", mp.pendingBatches[types.HexToHash("01")].whyNotReady(), msgList[len(msgList) - 1])
  }
  select {
  case <-ch:
  default:
    t.Errorf("Expected item on channel, nothing yet (%v) - %v", mp.pendingBatches[types.HexToHash("01")].whyNotReady(), msgList[len(msgList) - 1])
  }
  select {
  case v := <-ch:
    t.Errorf("Unexpected item on channel: %v", v)
  default:
  }
}
