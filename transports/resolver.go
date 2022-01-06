package transports

import (
  "bytes"
  "fmt"
  "math/big"
  "regexp"
  "strings"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/delivery"
)

func ResolveProducer(brokerURL, defaultTopic string, schema map[string]string) (Producer, error) {
  switch protocol := strings.Split(strings.TrimPrefix(brokerURL, "cardinal://"), "://"); protocol[0] {
  case "kafka":
    return NewKafkaProducer(strings.TrimPrefix(brokerURL, "cardinal://"), defaultTopic, schema)
  default:
    return nil, fmt.Errorf("unknown producer protocol '%v'", protocol)
  }
}


//brokerURL, defaultTopic string, topics []string, resumption []byte, rollback, lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp, whitelist map[uint64]types.Hash
func ResolveConsumer(brokerURL, defaultTopic string, topics []string, resumption []byte, rollback, lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp, whitelist map[uint64]types.Hash) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(lastNumber, lastHash, lastWeight, reorgThreshold, trackedPrefixes, whitelist)
  if err != nil { return nil, err }
  return resolveConsumer(omp, brokerURL, defaultTopic, topics, resumption, rollback, lastHash)
}

func resolveConsumer(omp *delivery.OrderedMessageProcessor, brokerURL, defaultTopic string, topics []string, resumption []byte, rollback int64, lastHash types.Hash) (Consumer, error) {
  switch protocol := strings.Split(strings.TrimPrefix(brokerURL, "cardinal://"), "://"); protocol[0] {
  case "kafka":
    return kafkaConsumerWithOMP(omp, brokerURL, defaultTopic, topics, resumption, rollback, lastHash)
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
func ResolveMuxConsumer(brokerParams []BrokerParams, resumption []byte, lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp, whitelist map[uint64]types.Hash) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(lastNumber, lastHash, lastWeight, reorgThreshold, trackedPrefixes, whitelist)
  if err != nil { return nil, err }
  mc := &muxConsumer{
    omp: omp,
    consumers: []Consumer{},
  }
  for _, bp := range brokerParams {
    c, err := resolveConsumer(omp, bp.URL, bp.DefaultTopic, bp.Topics, resumption, bp.Rollback, lastHash)
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


type muxConsumer struct {
  omp       *delivery.OrderedMessageProcessor
  consumers []Consumer
}

func (mc *muxConsumer) Start() error {
  for _, c := range mc.consumers {
    if err := c.Start(); err != nil { return err }
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
  go func() {
    for _, c := range mc.consumers {
      <-c.Ready()
    }
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
