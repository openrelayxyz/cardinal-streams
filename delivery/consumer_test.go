package delivery

import (
  "bytes"
  "testing"
  "math/big"
  "github.com/openrelayxyz/cardinal-types"
  // "strings"
  "regexp"
  "runtime"
)


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
  for _, msgList := range msgs{
    for _, msg := range msgList {
      if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
    }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    t.Errorf("Expected nothing on channel yet, got %v, %v, %v", v.PendingBatches, v.Prefixes, v.Batches)
  default:
  }

  msgs, err = p.SendBatch(types.HexToHash("ff"), []string{"whatever/", "other/"}, map[string][]byte{"state/thing": []byte("thing!")})
  if err != nil { t.Fatalf(err.Error()) }
  for _, msgList := range msgs{
    for _, msg := range msgList {
      if err := mp.ProcessMessage(msg); err != nil { t.Errorf(err.Error()) }
    }
  }
  runtime.Gosched()
  select {
  case v := <-ch:
    if v.Number != 0 { t.Errorf("Unexpected batch number") }
    if !bytes.Equal(v.Weight, new(big.Int).Bytes()) { t.Errorf("Unexpected weight") }
    if v.ParentHash != types.HexToHash("00") { t.Errorf("Unexpected hash" ) }
    if l := len(v.Values); l != 4 { t.Errorf("Unexpected updates length; Expected 4, got %v", l)} // 2 prefixes, 1 batch, 2 changes not in schema
    if l := len(v.Deletes); l != 5 { t.Errorf("Unexpected deletes length; Expected 5, got %v", v.Deletes)} // 2 prefixes, 1 batch, 2 changes not in schema
  default:
    t.Errorf("Expected item on channel, nothing yet")
  }
}
