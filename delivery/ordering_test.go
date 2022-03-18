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

func (tu *testUpdate) Messages(t *testing.T, p *Producer) ([]ResumptionMessage) {
  msgs, err := p.AddBlock(tu.number, tu.hash, tu.parentHash, tu.weight, tu.updates, tu.deletes, tu.batchIds())
  if err != nil { t.Fatalf(err.Error()) }
  result := toTestResumptionMessage(msgs)
  for _, b := range tu.batches {
    bmsgs, err := p.SendBatch(b.id, b.delete, b.update,)
    if err != nil { t.Fatalf(err.Error()) }
    result = append(result, toTestResumptionMessage(bmsgs)...)
  }
  return result
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
  messages:= b.Messages(t, p)
  cmessages:= c.Messages(t, p)
  messages = append(messages, cmessages...)
  rand.Seed(time.Now().UnixNano())
  rand.Shuffle(len(messages), func(i, j int) { messages[i], messages[j] = messages[j], messages[i] })
  omp, err := NewOrderedMessageProcessor(a.number, a.hash, a.weight, 128, []*regexp.Regexp{regexp.MustCompile(".")}, nil)
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

type expectedUpdate struct{
  added []types.Hash
  removed []types.Hash
}

func hashList(x []*PendingBatch) []types.Hash {
  result := make([]types.Hash, len(x))
  for i, pb := range x {
    result[i] = pb.Hash
  }
  return result
}

func expectedUpdateList(x []*ChainUpdate) []expectedUpdate {
  result := make([]expectedUpdate, len(x))
  for i, update := range x{
    result[i] = expectedUpdate{
      added: hashList(update.added),
      removed: hashList(update.removed),
    }
  }
  return result
}

func reorgTester(t *testing.T, messages []ResumptionMessage, expectedEvents []expectedUpdate, last *testUpdate) {
  outputs := []*ChainUpdate{}
  omp, err := NewOrderedMessageProcessor(last.number, last.hash, last.weight, 128, []*regexp.Regexp{regexp.MustCompile(".")}, nil)
  if err != nil { t.Fatalf(err.Error()) }
  ch := make(chan *ChainUpdate, 5)
  sub := omp.Subscribe(ch)
  defer sub.Unsubscribe()
  go func() {
    for {
      select {
      case <-sub.Err():
        return
      case update := <-ch:
        outputs = append(outputs, update)
      }
    }
  }()
  for _, msg := range messages {
    if err := omp.ProcessMessage(msg); err != nil {
      t.Errorf(err.Error())
    }
  }
  runtime.Gosched()
  omp.Close()
  fmt.Printf("queued: %v\n", omp.queued)

  // log.Info("Results", "outputs", expectedUpdateList(outputs), "leno", len(outputs), "expected", expectedEvents, "lene", len(expectedEvents), "b", len(outputs) == len(expectedEvents))
  if len(outputs) != len(expectedEvents) {
    newBlockNums := make([][]int64, len(outputs))
    revertedBlockNums := make([][]int64, len(outputs))
    for i, ces := range outputs {
      newBlockNums[i] = make([]int64, len(ces.added))
      revertedBlockNums[i] = make([]int64, len(ces.removed))
      for j, ce := range ces.added {
        newBlockNums[i][j] = ce.Number
      }
      for j, ce := range ces.removed {
        revertedBlockNums[i][j] = ce.Number
      }
    }
    t.Fatalf("Expected %v outputs, got %v (%v / %v)", len(expectedEvents), len(outputs), newBlockNums, revertedBlockNums)
  }
  for i, chainEvents := range expectedEvents {
    if len(chainEvents.added) != len(outputs[i].added) { t.Fatalf("Expected events[%v]: %v added, got %v (%v != %v)", i, len(chainEvents.added), len(outputs[i].added), chainEvents.added, hashList(outputs[i].added))}
    for j, hash := range chainEvents.added {
      if hash != outputs[i].added[j].Hash { t.Errorf("Got new chain events out of order o[%v][%v] expected %v got %v", i, j, hash, outputs[i].added[j].Hash) }
    }
    if len(chainEvents.removed) != len(outputs[i].removed) { t.Fatalf("Expected events[%v]: %v Reverted, got %v", i, len(chainEvents.removed), len(outputs[i].removed))}
    for j, hash := range chainEvents.removed {
      if hash != outputs[i].removed[j].Hash { t.Errorf("Got reverted chain events out of order o[%v][%v]", i, j) }
    }
  }
}

func assembleMessages(t *testing.T, p *Producer, updates ...*testUpdate) []ResumptionMessage {
  results := []ResumptionMessage{}
  for _, m := range updates {
    results = append(results, m.Messages(t, p)...)
  }
  return results
}

func TestReorg(t *testing.T) {
  p, err := NewProducer(
    "default",
    map[string]string{
      "foo/": "foo",
      "bar/[^/]+/baz/": "bar",
      "state/": "state",
    },
  )

  if err != nil { t. Fatalf(err.Error()) }

  a := getTestBlock(0, 0, nil)
  chain1 := []*testUpdate{a}
  chain2 := []*testUpdate{a}
  for i := 1; i < 100; i ++ {
    chain1 = append(chain1, getTestBlock(int64(i), 1, chain1[i-1]))
    chain2 = append(chain2, getTestBlock(int64(i), 2, chain2[i-1]))
  }
  b := getTestBlock(1, 0, a)
  c := getTestBlock(1, 1, a)
  d := getTestBlock(2, 1, c)
  e := getTestBlock(2, 0, b)
  f := getTestBlock(3, 0, e)
  g := getTestBlock(2, 2, b)
  h := getTestBlock(3, 1, g)
  i := getTestBlock(4, 0, h)
  j := getTestBlock(4, 1, f)
  k := getTestBlock(5, 0, i)

  // g := getTestBlock(4, 0, f.Block.Header())
  if b.parentHash != a.hash { t.Fatalf("b should be child of a") }
  if c.parentHash != a.hash { t.Fatalf("c should be child of a") }
  if d.parentHash != c.hash { t.Fatalf("d should be child of c") }
  if e.parentHash != b.hash { t.Fatalf("e should be child of b") }
  if f.parentHash != e.hash { t.Fatalf("f should be child of e") }
  if g.parentHash != b.hash { t.Fatalf("g should be child of b") }
  if h.parentHash != g.hash { t.Fatalf("h should be child of g") }
  if i.parentHash != h.hash { t.Fatalf("i should be child of h") }
  if j.parentHash != f.hash { t.Fatalf("j should be child of f") }
  if k.parentHash != i.hash { t.Fatalf("k should be child of i") }
  // if g.Block.ParentHash() != f.Hash { t.Fatalf("g should be child of e") }

  // // TODO: Try more out-of-order messages (instead of whole out-of-order blocks)
  t.Run("Reorg ABCD", func(t *testing.T) {
    // log.Info("Hashes", "a", a.hash, "b", b.hash, "c", c.hash, "d", d.hash)
    reorgTester(
      t,
      assembleMessages(t, p, a, b, c, d),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{b.hash}},
        expectedUpdate{added: []types.Hash{c.hash, d.hash}, removed: []types.Hash{b.hash}},
      },
      a,
    )
  })
  t.Run("Reorg ABDC", func(t *testing.T) {
    reorgTester(
      t,
      assembleMessages(t, p, a, b, d, c),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{b.hash}},
        expectedUpdate{added: []types.Hash{c.hash, d.hash}, removed: []types.Hash{b.hash}},
      },
      a,
    )
  })
  t.Run("Reorg ACDB", func(t *testing.T) {
    reorgTester(
      t,
      assembleMessages(t, p, a, c, d, b),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{c.hash}},
        expectedUpdate{added: []types.Hash{d.hash}},
      },
      a,
    )
  })
  t.Run("Reorg ABCDEF", func(t *testing.T) {
    reorgTester(
      t,
      assembleMessages(t, p, a, b, c, d, e, f),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{b.hash}},
        expectedUpdate{added: []types.Hash{c.hash, d.hash}, removed: []types.Hash{b.hash}},
        expectedUpdate{added: []types.Hash{b.hash, e.hash, f.hash}, removed: []types.Hash{c.hash, d.hash}},
      },
      a,
    )
  })
  t.Run("Reorg AEGFBHI", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, e, g, f, b, h, i),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash, e.hash, f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash}, removed: []types.Hash{e.hash, f.hash}},
        },
        a,
      )
  })
  t.Run("Reorg AEGFHIB", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, e, g, f, h, i, b),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash, g.hash, h.hash, i.hash}, removed: []types.Hash{}},
        },
        a,
      )
  })
  t.Run("Reorg AEFHIGB", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, e, f, h, i, g, b),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash, g.hash, h.hash, i.hash}, removed: []types.Hash{}},
        },
        a,
      )
  })
  t.Run("Reorg ABEFJHIKG", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, b, e, f, j, h, i, k, g),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{e.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{j.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash, k.hash}, removed: []types.Hash{e.hash, f.hash, j.hash}},
        },
        a,
      )
  })
  t.Run("Reorg ABEFJGIKH", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, b, e, f, j, g, i, k, h),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{e.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{j.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash, k.hash}, removed: []types.Hash{e.hash, f.hash, j.hash}},
        },
        a,
      )
  })
  t.Run("Reorg ABEFJGHIK", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, b, e, f, j, g, h, i, k),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{e.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{j.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash, k.hash}, removed: []types.Hash{e.hash, f.hash, j.hash}},
        },
        a,
      )
  })
  t.Run("Reorg ABEFJIKHG", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, b, e, f, j, i, k, h, g),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{e.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{j.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash, k.hash}, removed: []types.Hash{e.hash, f.hash, j.hash}},
        },
        a,
      )
  })
  t.Run("Reorg ABEFJGHKI", func(t *testing.T) {
      reorgTester(
        t,
        assembleMessages(t, p, a, b, e, f, j, g, h, k, i),
        []expectedUpdate{
          expectedUpdate{added: []types.Hash{b.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{e.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{f.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{j.hash}, removed: []types.Hash{}},
          expectedUpdate{added: []types.Hash{g.hash, h.hash, i.hash, k.hash}, removed: []types.Hash{e.hash, f.hash, j.hash}},
        },
        a,
      )
  })
  t.Run("Reorg adc", func(t *testing.T) {
    reorgTester(
      t,
      assembleMessages(t, p, a, d, c),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{c.hash, d.hash}, removed: []types.Hash{}},
      },
      a,
    )
  })
  t.Run("Reorg start(c) ACDBEF", func(t *testing.T) {
    reorgTester(
      t,
      assembleMessages(t, p, a, c, d, b, e, f),
      []expectedUpdate{
        expectedUpdate{added: []types.Hash{d.hash}, removed: []types.Hash{}},
        expectedUpdate{added: []types.Hash{b.hash, e.hash, f.hash}, removed: []types.Hash{c.hash, d.hash}},
      },
      c,
    )
  })
  t.Run("Reorg chain1[:50] -> chain2[:53]", func(t *testing.T) {
    expectations := []expectedUpdate{}
    for _, item := range chain1[1:50] {
      expectations = append(
        expectations,
        expectedUpdate{added: []types.Hash{item.hash}, removed: []types.Hash{}},
      )
    }
    finalExpectation := expectedUpdate{
      added: []types.Hash{},
      removed: []types.Hash{},
    }
    for _, item := range chain2[1:54] {
      finalExpectation.added = append(finalExpectation.added, item.hash)
    }
    for _, item := range chain1[1:50] {
      finalExpectation.removed = append(finalExpectation.removed, item.hash)
    }
    reorgTester(
      t,
      append(append(append(assembleMessages(t, p, chain1[:50]...), assembleMessages(t, p, chain2[:25]...)...), assembleMessages(t, p, chain2[30:53]...)...), assembleMessages(t, p, chain2[25], chain2[26], chain2[27], chain2[28], chain2[53], chain2[29])...),
      append(expectations, finalExpectation),
      a,
    )
  })
  t.Run("Reorg chain1[:6] -> chain2[5,6,3,4,1,2]", func(t *testing.T) {
    expectations := []expectedUpdate{}
    for _, item := range chain1[1:6] {
      expectations = append(
        expectations,
        expectedUpdate{added: []types.Hash{item.hash}, removed: []types.Hash{}},
      )
    }
    finalExpectation := expectedUpdate{
      added: []types.Hash{},
      removed: []types.Hash{},
    }
    for _, item := range chain2[1:7] {
      finalExpectation.added = append(finalExpectation.added, item.hash)
    }
    for _, item := range chain1[1:6] {
      finalExpectation.removed = append(finalExpectation.removed, item.hash)
    }
    reorgTester(
      t,
      append(assembleMessages(t, p, chain1[:6]...), assembleMessages(t, p, chain2[5], chain2[6], chain2[3], chain2[4], chain2[1], chain2[2])...),
      append(expectations, finalExpectation),
      a,
    )
  })
  t.Run("Startup Reorg", func(t *testing.T) {
    finalExpectation := expectedUpdate{
      added: []types.Hash{},
      removed: []types.Hash{},
    }
    for _, item := range chain2[1:8] {
      finalExpectation.added = append(finalExpectation.added, item.hash)
    }
    for _, item := range chain1[1:7] {
      finalExpectation.removed = append(finalExpectation.removed, item.hash)
    }
    expectations := []expectedUpdate{finalExpectation}
    reorgTester(
      t,
      append(assembleMessages(t, p, chain1[:7]...), assembleMessages(t, p, chain2[:8]...)...),
      expectations,
      chain1[6],
    )
  })
  t.Run("Startup Reorg OO", func(t *testing.T) {
    finalExpectation := expectedUpdate{
      added: []types.Hash{},
      removed: []types.Hash{},
    }
    for _, item := range chain2[1:9] {
      finalExpectation.added = append(finalExpectation.added, item.hash)
    }
    for _, item := range chain1[1:7] {
      finalExpectation.removed = append(finalExpectation.removed, item.hash)
    }
    expectations := []expectedUpdate{finalExpectation}
    reorgTester(
      t,
      append(append(assembleMessages(t, p, chain1[:6]...), assembleMessages(t, p, chain2[:8]...)...), assembleMessages(t, p, chain1[6], chain2[8])...),
      expectations,
      chain1[6],
    )
  })
}
