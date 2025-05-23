package transports

import (
  "bytes"
  "fmt"
  "math/big"
  "regexp"
  "time"
  "strings"
  "reflect"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/v2/delivery"
  "github.com/openrelayxyz/cardinal-streams/v2/waiter"
  log "github.com/inconshreveable/log15"
)

func ResolveProducer(brokerURL, defaultTopic string, schema map[string]string) (Producer, error) {
  log.Warn("ResolveProducer is deprecated. Use ResolveProducerWithResumer for more supported protocols.")
  switch protocol := strings.Split(strings.TrimPrefix(brokerURL, "cardinal://"), "://"); protocol[0] {
  case "kafka":
    return NewKafkaProducer(strings.TrimPrefix(brokerURL, "cardinal://"), defaultTopic, schema)
  default:
    return nil, fmt.Errorf("unknown producer protocol '%v'", protocol)
  }
}

func ResolveProducerWithResumer(brokerURL, defaultTopic string, schema map[string]string, resumer StreamsResumption) (Producer, error) {
	switch protocol := strings.Split(strings.TrimPrefix(brokerURL, "cardinal://"), "://"); protocol[0] {
  case "ws", "wss":
    return NewWebsocketProducer(strings.TrimPrefix(brokerURL, "cardinal://"), resumer)
  case "kafka":
    return NewKafkaProducer(strings.TrimPrefix(brokerURL, "cardinal://"), defaultTopic, schema)
  default:
    return nil, fmt.Errorf("unknown producer protocol '%v'", protocol)
  }
}

func ResolveMuxProducer(brokerParams []ProducerBrokerParams, resumer StreamsResumption) (Producer, error) {
	mp := &muxProducer{
		producers: make([]Producer, len(brokerParams)),
	}
	for i, bp := range brokerParams {
		producer, err := ResolveProducerWithResumer(bp.URL, bp.DefaultTopic, bp.Schema, resumer)
		if err != nil { return nil, err }
		mp.producers[i] = producer
	}
	return mp, nil
}


//brokerURL, defaultTopic string, topics []string, resumption []byte, rollback, lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp, whitelist map[uint64]types.Hash
func ResolveConsumer(brokerURL, defaultTopic string, topics []string, resumption []byte, rollback int64, cfg *delivery.ConsumerConfig) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(cfg)
  if err != nil { return nil, err }
  return resolveConsumer(omp, brokerURL, defaultTopic, topics, resumption, rollback, cfg.LastEmittedNum, cfg.LastHash, cfg.TrackedPrefixes, cfg.Blacklist)
}

func resolveConsumer(omp *delivery.OrderedMessageProcessor, brokerURL, defaultTopic string, topics []string, resumption []byte, rollback, lastNumber int64, lastHash types.Hash, trackedPrefixes []*regexp.Regexp, blacklist map[string]map[int32]map[int64]struct{}) (Consumer, error) {
  switch protocol := strings.Split(strings.TrimPrefix(brokerURL, "cardinal://"), "://"); protocol[0] {
  case "kafka":
    return kafkaConsumerWithOMP(omp, brokerURL, defaultTopic, topics, resumption, rollback, lastHash, blacklist)
  case "ws", "wss":
    prefixes := make([]string, len(trackedPrefixes))
    for i, p := range trackedPrefixes {
      prefixes[i] = p.String()
    }
    return newWebsocketConsumer(omp, brokerURL, lastNumber, lastHash)
  case "null":
    return NewNullConsumer(), nil
  default:
    return nil, fmt.Errorf("unknown consumer protocol '%v'", protocol)
  }
}

func ResumptionForTimestamp(brokerParams []BrokerParams, timestamp int64) ([]byte, error) {
  results := [][]byte{}
  for _, bp := range brokerParams {
    switch protocol := strings.Split(strings.TrimPrefix(bp.URL, "cardinal://"), "://"); protocol[0] {
    case "kafka":
      res, err := kafkaResumptionForTimestamp(bp.URL, bp.Topics, timestamp)
      if err != nil {
        return nil, err
      }
      results = append(results, res)
    case "null":
    default:
      return nil, fmt.Errorf("unknown consumer protocol '%v'", protocol)
    }
  }
  return bytes.Join(results, []byte(";")), nil
}

// ResolveMuxConsumer takes a list of broker configurations
func ResolveMuxConsumer(brokerParams []BrokerParams, resumption []byte, cfg *delivery.ConsumerConfig) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(cfg)
  if err != nil { return nil, err }
  mc := &muxConsumer{
    omp: omp,
    consumers: []Consumer{},
  }
  for _, bp := range brokerParams {
    c, err := resolveConsumer(omp, bp.URL, bp.DefaultTopic, bp.Topics, resumption, bp.Rollback, cfg.LastEmittedNum, cfg.LastHash, cfg.TrackedPrefixes, cfg.Blacklist)
    if err != nil { return nil, err }
    mc.consumers = append(mc.consumers, c)
  }
  return mc, nil
}

type BrokerParams struct {
  URL          string   `yaml:"URL"`
  DefaultTopic string   `yaml:"DefaultTopic"`
  Topics       []string `yaml:"Topics"`
  Rollback     int64    `yaml:"Rollback"`
}

type ProducerBrokerParams struct {
	URL string               `yaml:"URL"`
	DefaultTopic string      `yaml:"DefaultTopic"`
	Schema map[string]string `yaml:"Schema"`
}


type muxConsumer struct {
  omp       *delivery.OrderedMessageProcessor
  consumers []Consumer
  waiter    waiter.Waiter
}

func (mc *muxConsumer) Waiter() waiter.Waiter {
  if mc.waiter == nil {
    for _, c := range mc.consumers {
      if w := c.Waiter(); w != (nullWaiter{}) {
        mc.waiter = w
        break
      }
    }
    if mc.waiter == nil {
      mc.waiter = nullWaiter{}
    }
  }
  return mc.waiter
}

func (mc *muxConsumer) ProducerCount(d time.Duration) uint {
  var count uint
  for _, c := range mc.consumers {
    count += c.ProducerCount(d)
  }
  return count
}

func (mc *muxConsumer) Start() error {
  var consumerErrs int
  for _, c := range mc.consumers {
    err := c.Start() 
    if err != nil { 
      consumerErrs += 1
      log.Error("Error received from call to consumer.start()", "consumer", c)
    }
  }
  if consumerErrs == len(mc.consumers) {
    return fmt.Errorf("All consumers returned an error")
  }
  return nil
}
func (mc *muxConsumer) Subscribe(ch interface{}) types.Subscription {
  return mc.omp.Subscribe(ch)
}
func (mc *muxConsumer) SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription {
  return mc.omp.SubscribeReorg(ch)
}
func (mc *muxConsumer) Close() {
  for _, c := range mc.consumers {
    c.Close()
  }
}

func (mc *muxConsumer) Ready() <-chan struct{} {
  ch := make(chan struct{})
  // go func() {
  //   for _, c := range mc.consumers {
  //     select {
  //     case <-c.Ready():
  //       ch <- struct{}{}
  //       return
  //     }
  //   }
  // }()
  // return ch

  go func() {
    channels := make([]reflect.SelectCase, len(mc.consumers))
    for i, c := range mc.consumers {
      channels[i] = reflect.SelectCase{
        Dir: reflect.SelectRecv,
        Chan: reflect.ValueOf(c.Ready()),
      }
    }
    reflect.Select(channels)
    ch <- struct{}{}
  }()
  return ch
}

func (mc *muxConsumer) WhyNotReady(h types.Hash) string {
  for _, c := range mc.consumers {
    if v := c.WhyNotReady(h); v != "" {
      return v
    }
  }
  return "no consumers to report"
}


// LastHash types.Hash
// LastNumber int64
// LastWeight *big.Int
// TrackedPrefixes []*regexp.Regexp


type muxProducer struct {
  producers []Producer
}

func (mc *muxProducer) ProducerCount(d time.Duration) uint {
  var count uint
  for _, c := range mc.producers {
    count += c.ProducerCount(d)
  }
  return count
}

func (mc *muxProducer) SetHealth(b bool) {
  for _, p := range mc.producers {
    p.SetHealth(b)
  }
}

func (mp *muxProducer) LatestBlockFromFeed() (int64, error) {
	n := int64(9223372036854775807)
	var topErr error
	for _, p := range mp.producers {
		v, err := p.LatestBlockFromFeed()
		if err != nil {
			topErr = err
			continue
		}
		if err == nil && v < n {
			n = v
		}
	}
	if n < int64(9223372036854775807) {
		return n, nil
	}
	return 0, topErr
}

func (mp *muxProducer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error {
	errs := []error{}
	for _, p := range mp.producers {
		if err := p.AddBlock(number, hash, parentHash, weight, updates, deletes, batches); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 { return errs[0] }
	return nil
}

func (mp *muxProducer) SendBatch(batchid types.Hash, delete []string, update map[string][]byte) error {
	errs := []error{}
	for _, p := range mp.producers {
		if err := p.SendBatch(batchid, delete, update); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 { return errs[0] }
	return nil
}

func (mp *muxProducer) Reorg(number int64, hash types.Hash) (func(), error) {
	errs := []error{}
	fns := []func(){}
	for _, p := range mp.producers {
		fn, err := p.Reorg(number, hash)
		if err != nil {
			errs = append(errs, err)
		} else {
			fns = append(fns, fn)
		}
	}
	fn := func() {
		for _, fn := range fns {
			fn()
		}
	}
	if len(errs) > 0 { return fn, errs[0] }
	return fn, nil
}

func (mp *muxProducer) PurgeReplayCache() {
  for _, p := range mp.producers{
    p.PurgeReplayCache()
  }
}