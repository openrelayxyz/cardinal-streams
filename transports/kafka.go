package transports

import (
	"bytes"
	"fmt"
	coreLog "log"
	"math/big"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/v2/delivery"
	"github.com/openrelayxyz/cardinal-streams/v2/waiter"
	types "github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-types/metrics"
	"github.com/openrelayxyz/drumline"
)

var (
  skippedBlocks = metrics.NewMinorHistogram("/streams/skipped")
  producerCount = metrics.NewMajorGauge("/streams/producers")
)

type pingTracker map[types.Hash]time.Time

func (pt pingTracker) update(key types.Hash) {
  pt[key] = time.Now()
}

func (pt pingTracker) ProducerCount(d time.Duration) uint {
  var count uint
  for _, v := range pt {
    if time.Since(v) < d {
      count++
    }
  }
  producerCount.Update(int64(count))
  return count
}

type KafkaResumptionMessage struct {
  msg *sarama.ConsumerMessage
}

func (m *KafkaResumptionMessage) Offset() int64 {
  return m.msg.Offset
}
func (m *KafkaResumptionMessage) Source() string {
  return fmt.Sprintf("%v:%v", m.msg.Topic, m.msg.Partition)
}
func (m *KafkaResumptionMessage) Key() []byte {
  return m.msg.Key
}
func (m *KafkaResumptionMessage) Value() []byte {
  return m.msg.Value
}
func (m *KafkaResumptionMessage) Time() time.Time {
  return m.msg.Timestamp
}

func ParseKafkaURL(brokerURL string) ([]string, *sarama.Config) {
  parsedURL, _ := url.Parse("kafka://" + strings.TrimPrefix(brokerURL, "kafka://"))
  config := sarama.NewConfig()
  config.Version = sarama.V2_5_0_0

  if parsedURL.Query().Get("tls") == "1" {
    config.Net.TLS.Enable = true
  }
  if val := parsedURL.Query().Get("fetch.default"); val != "" {
    fetchDefault, err := strconv.Atoi(val)
    if err != nil {
      log.Warn("fetch.default set, but not number", "fetch.default", val)
    } else {
      config.Consumer.Fetch.Default = int32(fetchDefault)
    }
  }
  if val := parsedURL.Query().Get("max.waittime"); val != "" {
    maxWaittime, err := strconv.Atoi(val)
    if err != nil {
      log.Warn("max.waittime set, but not number", "max.waittime", val)
    } else {
      config.Consumer.MaxWaitTime = time.Duration(maxWaittime) * time.Millisecond
    }
  }
  switch parsedURL.Query().Get("compression.codec") {
  case "gzip":
    config.Producer.Compression = sarama.CompressionGZIP
  case "none":
    config.Producer.Compression = sarama.CompressionNone
  case "lz4":
    config.Producer.Compression = sarama.CompressionLZ4
  case "zstd":
    config.Producer.Compression = sarama.CompressionZSTD
  case "snappy":
    config.Producer.Compression = sarama.CompressionSnappy
  default:
    log.Warn("compression.codec not set or not recognized. Defaulting to snappy")
    config.Producer.Compression = sarama.CompressionSnappy
  }

  if val, err := strconv.Atoi(parsedURL.Query().Get("message.send.max.retries")); err == nil {
    config.Producer.Retry.Max = val
  } else {
    config.Producer.Retry.Max = 100
  }
  if val, err := strconv.Atoi(parsedURL.Query().Get("request.required.acks")); err == nil {
    config.Producer.RequiredAcks = sarama.RequiredAcks(val)
  } else {
    config.Producer.RequiredAcks = sarama.WaitForAll
  }

  if val, err := strconv.Atoi(parsedURL.Query().Get("retry.backoff.ms")); err == nil {
    config.Producer.Retry.Backoff = time.Duration(val) * time.Millisecond
  }
  if val, err := strconv.Atoi(parsedURL.Query().Get("net.maxopenrequests")); err == nil {
    config.Net.MaxOpenRequests = val
  }
  if parsedURL.Query().Get("idempotent") == "1" {
    config.Producer.Idempotent = true
  }
  if parsedURL.Query().Get("log") == "1" {
    sarama.Logger = coreLog.New(os.Stderr, "", coreLog.LstdFlags)
  }

  if parsedURL.User != nil {
    config.Net.SASL.Enable = true
    config.Net.SASL.User = parsedURL.User.Username()
    config.Net.SASL.Password, _ = parsedURL.User.Password()
  }
  switch parsedURL.Query().Get("sasl.mechanism") {
  case "SCRAM-SHA-256":
    config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
    config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
  case "SCRAM-SHA-512":
    config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
    config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
  case "GSSAPI":
    config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
  default:
  }
  return strings.Split(parsedURL.Host, ","), config
}


func CreateTopicIfDoesNotExist(brokerAddr, topic string, numPartitions int32, configEntries map[string]*string) error {
  if topic == "" {
    return fmt.Errorf("Unspecified topic")
  }
  if configEntries == nil {
    configEntries = make(map[string]*string)
  }
  brokerList, config := ParseKafkaURL(brokerAddr)
  client, err := sarama.NewClient(brokerList, config)
  if err != nil {
    return err
  }
  defer client.Close()
  broker, err := client.Controller()
  if err != nil {
    return err
  }
  log.Info("Getting metadata")
  broker.Open(config)
  defer broker.Close()
  response, err := broker.GetMetadata(&sarama.MetadataRequest{Topics: []string{topic}})
  // log.Info("Got here", "err", err, "topics", *response.Topics[0])
  if err != nil {
    log.Error("Error getting metadata", "err", err)
    return err
  }
  if len(response.Topics) == 0 || len(response.Topics[0].Partitions) == 0 {
    log.Info("Attempting to create topic")
    topicDetails := make(map[string]*sarama.TopicDetail)

    maxBytes := "10000024"
    configEntries["max.message.bytes"] = &maxBytes
    replicationFactor := int16(len(client.Brokers()))
    if replicationFactor > 3 {
      // If we have more than 3 brokers, only replicate to 3
      replicationFactor = 3
    }
    if numPartitions <= 0 {
      numPartitions = int32(len(client.Brokers())) * 2
    }
    topicDetails[topic] = &sarama.TopicDetail{
      ConfigEntries: configEntries,
      NumPartitions: numPartitions,
      ReplicationFactor: replicationFactor,
    }
    r, err := broker.CreateTopics(&sarama.CreateTopicsRequest{
      // Version: 2,
      Timeout: 5 * time.Second,
      TopicDetails: topicDetails,
    })
    if err != nil {
      log.Error("Error creating topic", "error", err, "response", r)
      return err
    }
    if err, _ := r.TopicErrors[topic]; err != nil && err.Err != sarama.ErrNoError {
      log.Error("topic error", "err", err)
      return err
    }
    log.Info("Topic created without errors")
  }
  return nil
}


type KafkaProducer struct{
  dp           *delivery.Producer
  producer     sarama.AsyncProducer
  reorgTopic   string
  defaultTopic string
  brokerURL    string
  recentHashes *lru.Cache
  skipBatches  *lru.Cache
  pt           pingTracker
  healthy      bool
}

func NewKafkaProducer(brokerURL, defaultTopic string, schema map[string]string) (Producer, error) {
  r := rand.New(rand.NewSource(time.Now().UnixNano())) // Not cryptographically secure, but fine for unique producer IDs
  var idBytes[32]byte
  r.Read(idBytes[:])
  dp, err := delivery.NewProducer(defaultTopic, schema)
  if err != nil { return nil, err }
  brokers, config := ParseKafkaURL(strings.TrimPrefix(brokerURL, "kafka://"))
  if err := CreateTopicIfDoesNotExist(brokerURL, defaultTopic, -1, nil); err != nil {
    return nil, err
  }
  for _, v := range schema {
    if err := CreateTopicIfDoesNotExist(brokerURL, v, -1, nil); err != nil {
      return nil, err
    }
  }
  config.Producer.MaxMessageBytes = 10000024
  producer, err := sarama.NewAsyncProducer(brokers, config)
  if err != nil {
    return nil, err
  }
  pt := make(pingTracker)
  client, err := sarama.NewClient(brokers, config)
  if err != nil { return nil, err }
  consumer, err := sarama.NewConsumerFromClient(client)
  if err != nil { return nil, err }
  partitions, err := consumer.Partitions(defaultTopic)
  if err != nil { return nil, err }
  hashCh := make(chan types.Hash)
  heartbeats := make(chan types.Hash)
  var readyWg sync.WaitGroup
  for _, partid := range partitions {
    readyWg.Add(1)
    go func(partid int32, readyWg *sync.WaitGroup) {
      var once sync.Once
      offset, err := client.GetOffset(defaultTopic, partid, sarama.OffsetNewest)
      if err != nil {
        offset = sarama.OffsetNewest
        once.Do(readyWg.Done)
      } else if offset == 0 {
        offset = sarama.OffsetNewest
        once.Do(readyWg.Done)
      }
      offset -= 100
      if offset < 0 { offset = sarama.OffsetOldest }
      pc, err := consumer.ConsumePartition(defaultTopic, partid, offset)
      if err != nil {
        pc, err = consumer.ConsumePartition(defaultTopic, partid, sarama.OffsetOldest)
        if err != nil {
          once.Do(readyWg.Done)
          return
        }
      }
      PARTITION_LOOP:
      for {
        select {
        case input := <-pc.Messages():
          if input == nil { break PARTITION_LOOP }
          if hwm := pc.HighWaterMarkOffset(); hwm - input.Offset <= 1 {
            once.Do(readyWg.Done)
          }
          if len(input.Key) == 0 { continue PARTITION_LOOP }
          switch delivery.MessageType(input.Key[0]) {
          case delivery.BatchType:
            hashCh <- types.BytesToHash(input.Key[1:33])
          case delivery.PingType:
            if !bytes.Equal(input.Key[1:], idBytes[:]) { // Ignore our own heartbeats
              heartbeats <- types.BytesToHash(input.Key[1:])
            }
          }
        }
      }
    }(partid, &readyWg)
  }
  cache, _ := lru.New(4096)
  skipBatches, _ := lru.New(1024)
  kp := &KafkaProducer{dp: dp, producer: producer, reorgTopic: "", defaultTopic: defaultTopic, brokerURL: brokerURL, recentHashes: cache, skipBatches: skipBatches, pt: pt, healthy: true}
  go func() {
    ticker := time.NewTicker(30 * time.Second)
    for {
      select {
      case t := <-ticker.C:
        b, _ := t.MarshalBinary()
        if kp.healthy {
            producer.Input() <- &sarama.ProducerMessage{Topic: defaultTopic, Key: sarama.ByteEncoder(delivery.PingType.GetKey(idBytes[:])), Value: sarama.ByteEncoder(b)}
        }
      case h := <-hashCh:
        cache.Add(h, struct{}{})
      case h := <-heartbeats:
        kp.pt.update(h)
      }
    }
  }()
  readyWg.Wait()
  return kp, nil
}

func (kp *KafkaProducer) ProducerCount(d time.Duration) uint {
  return kp.pt.ProducerCount(d)
}

func (kp *KafkaProducer) SetHealth(b bool) {
  kp.healthy = b
}

func (kp *KafkaProducer) emitBundle(bundle map[string][]delivery.Message) error {
  for topic, msgs := range bundle {
    if kp.reorgTopic != "" { topic = kp.reorgTopic }
    for _, msg := range msgs {
      if err := kp.emit(topic, msg); err != nil { return err }
    }
  }
  return nil
}

func (kp *KafkaProducer) emit(topic string, msg delivery.Message) error {
  select {
  case kp.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(msg.Key()), Value: sarama.ByteEncoder(msg.Value())}:
  case err := <-kp.producer.Errors():
    log.Error("Error emitting: %v", "err", err.Error())
    return err
  }
  return nil
}

func (kp *KafkaProducer) AddBlock(number int64, hash, parentHash types.Hash, weight *big.Int, updates map[string][]byte, deletes map[string]struct{}, batches map[string]types.Hash) error {
  if kp.recentHashes.Contains(hash) {
    // We saw this block come from another producer, we don't need to rebroadcast
    for _, v := range batches {
      kp.skipBatches.Add(v, struct{}{})
    }
    skippedBlocks.Update(1)
    return nil
  }
  msgs, err := kp.dp.AddBlock(number, hash, parentHash, weight, updates, deletes, batches)
  if err != nil { return err }
  skippedBlocks.Update(0)
  return kp.emitBundle(msgs)
}
func (kp *KafkaProducer) SendBatch(batchid types.Hash, delete []string, update map[string][]byte) error {
  msgs, err := kp.dp.SendBatch(batchid, delete, update)
  if err == delivery.ErrUnknownBatch && kp.skipBatches.Contains(batchid) {
    return nil
  }
  if err != nil { return err }
  return kp.emitBundle(msgs)
}
func (kp *KafkaProducer) Reorg(number int64, hash types.Hash) (func(), error) {
  // msg := kp.dp.Reorg(number, hash)
  // oldReorgTopic := kp.reorgTopic
  // kp.reorgTopic = fmt.Sprintf("%s-re-%x",  kp.defaultTopic, hash.Bytes())
  // done := func() {
  //   kp.emit(kp.reorgTopic, kp.dp.ReorgDone(number, hash))
  //   kp.reorgTopic = oldReorgTopic
  // }
  // if err := CreateTopicIfDoesNotExist(kp.brokerURL, kp.reorgTopic, 1, nil); err != nil {
  //   done()
  //   return func() {}, err
  // }
  // if err := kp.emit(kp.defaultTopic, msg); err != nil {
  //   done()
  //   return func(){}, err
  // }
  // kp.emit(kp.reorgTopic, msg)
  // return done, nil
  log.Warn("Large reorg functionality disabled")
  return func(){}, nil
}


// LatestBlockFromFeed scans the feed the producer is configured for and finds
// the latest block number. This should be used once on startup, and is
// intended to allow producers to sync to a particular block before the begin
// emitting messages. Producers should start emitting when they reach this
// number, to avoid skipped blocks (which will hault consumers). Producer
// applications should provide some kind of override, resuming at a block
// specified by an operator in case messages are needed to start on the correct
// side of a reorg while the feed has messages from a longer but invalid chain.
func (kp *KafkaProducer) LatestBlockFromFeed() (int64, error) {
  brokerList, config := ParseKafkaURL(kp.brokerURL)
  client, err := sarama.NewClient(brokerList, config)
  if err != nil { return 0, err }
  consumer, err := sarama.NewConsumerFromClient(client)
  if err != nil { return 0, err }
  partitions, err := consumer.Partitions(kp.defaultTopic)
  if err != nil { return 0, err }
  var highestNumber int64
  for _, partid := range partitions {
    offset, err := client.GetOffset(kp.defaultTopic, partid, sarama.OffsetNewest)
    if err != nil {
      log.Info("Failed to get offset", "topic", kp.defaultTopic, "partition", partid)
      continue
    }
    if offset == 0 { continue }
    offset -= 100
    if offset < 0 { offset = sarama.OffsetOldest }
    pc, err := consumer.ConsumePartition(kp.defaultTopic, partid, offset)
    if err != nil {
      pc, err = consumer.ConsumePartition(kp.defaultTopic, partid, sarama.OffsetOldest)
      if err != nil { return 0, err }
    }
    PARTITION_LOOP:
    for {
      select {
      case input := <-pc.Messages():
        if input == nil { break PARTITION_LOOP }
        if hwm := pc.HighWaterMarkOffset(); hwm - input.Offset <= 1 {
          pc.AsyncClose()
        }
        if len(input.Key) == 0 { continue PARTITION_LOOP }
        if delivery.MessageType(input.Key[0]) == delivery.BatchType {
          b, err := delivery.UnmarshalBatch(input.Value)
          if err != nil {
            log.Warn("Error unmarshalling batch", "err", err)
            continue
          }
          if b.Number > highestNumber { highestNumber = b.Number }
        }
      case <-time.NewTimer(time.Second).C:
        log.Info("No messages available on partition", "topic", kp.defaultTopic, "partition", partid)
        break PARTITION_LOOP
      }
    }
  }
  return highestNumber, nil
}

func (kp *KafkaProducer) PurgeReplayCache() {
  kp.recentHashes.Purge()
  kp.skipBatches.Purge()
}

type KafkaConsumer struct{
  omp          *delivery.OrderedMessageProcessor
  quit         chan struct{}
  ready        chan struct{}
  brokerURL    string
  defaultTopic string
  topics       []string
  resumption   map[string]int64
  client       sarama.Client
  rollback     int64
  shutdownWg   sync.WaitGroup
  isReorg      bool
  startHash    types.Hash
  pt           pingTracker
  waiter       waiter.Waiter
  blacklist    map[string]map[int32]map[int64]struct{}
}

func kafkaConsumerWithOMP(omp *delivery.OrderedMessageProcessor, brokerURL, defaultTopic string, topics []string, resumption []byte, rollback int64, lastHash types.Hash, blacklist map[string]map[int32]map[int64]struct{}) (Consumer, error) {
  resumptionMap := make(map[string]int64)
  for _, token := range strings.Split(string(resumption), ";") {
    parts := strings.Split(token, "=")
    if len(parts) != 2 { continue }
    offset, err := strconv.Atoi(parts[1])
    if err != nil { return nil, err }
    resumptionMap[parts[0]] = int64(offset)
  }
  brokers, config := ParseKafkaURL(strings.TrimPrefix(brokerURL, "kafka://"))
  client, err := sarama.NewClient(brokers, config)
  if err != nil {
    log.Error("error connecting to brokers", "brokers", brokers)
    return nil, err
  }
  return &KafkaConsumer{
    omp: omp,
    quit: make(chan struct{}),
    ready: make(chan struct{}),
    brokerURL: brokerURL,
    defaultTopic: defaultTopic,
    topics: topics,
    resumption: resumptionMap,
    client: client,
    rollback: rollback,
    startHash: lastHash,
    pt: make(pingTracker),
    blacklist: blacklist,
  }, nil
}



func kafkaResumptionForTimestamp(brokerURL string, topics []string, timestamp int64) ([]byte, error) {
  brokers, config := ParseKafkaURL(strings.TrimPrefix(brokerURL, "kafka://"))
  client, err := sarama.NewClient(brokers, config)
  if err != nil {
    log.Error("error connecting to brokers", "brokers", brokers)
    return nil, err
  }
  resumption := []string{}
  for _, topic := range topics {
    parts, err := client.Partitions(topic)
    if err != nil { return nil, err }
    for _, part := range parts {
      offset, err := client.GetOffset(topic, part, timestamp)
      if err != nil {
        return nil, err
      }
      resumption = append(resumption, fmt.Sprintf("%v:%v=%v", topic, part, offset))
    }
  }
  return []byte(strings.Join(resumption, ";")), nil
}

// NewKafkaConsumer provides a transports.Consumer that pulls messages from a
// Kafka broker
func NewKafkaConsumer(brokerURL, defaultTopic string, topics []string, resumption []byte, rollback int64, cfg *delivery.ConsumerConfig) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(cfg)
  if err != nil { return nil, err }
  return kafkaConsumerWithOMP(omp, brokerURL, defaultTopic, topics, resumption, rollback, cfg.LastHash, cfg.Blacklist)
}

func (kc *KafkaConsumer) Start() error {
  log.Debug("Starting kafka consumer")
  dl := drumline.NewDrumline(256)
  consumer, err := sarama.NewConsumerFromClient(kc.client)
  if err != nil { return err }
  var readyWg, warmupWg, reorgWg sync.WaitGroup
  messages := make(chan *sarama.ConsumerMessage, 512) // 512 is arbitrary. Worked for Flume, but may require tuning for Cardinal
  partitionConsumers := []sarama.PartitionConsumer{}
  heartbeats := make(chan types.Hash)
  for _, topic := range kc.topics {
    partitions, err := consumer.Partitions(topic)
    if err != nil { return err }
    for _, partid := range partitions {
      readyWg.Add(1)
      warmupWg.Add(1)
      k := fmt.Sprintf("%v:%v", topic, partid)
      offset, ok := kc.resumption[k]
      var startOffset int64
      if !ok || offset < kc.rollback{
        if kc.startHash == (types.Hash{}) && kc.rollback >= 0 {
          // An empty hash indicates an intention to start tracking the feed
          // now, so we should not add historical messages.
          offset, err = kc.client.GetOffset(topic, partid, sarama.OffsetNewest)
          if err != nil {
            offset = sarama.OffsetNewest
            startOffset = offset
          } else {
            startOffset = offset - 1
          }
        } else {
          offset = sarama.OffsetOldest
          startOffset = offset
        }
      } else {
        startOffset = offset - kc.rollback
      }
      pc, err := consumer.ConsumePartition(topic, partid, startOffset)
      if err != nil {
        pc, err = consumer.ConsumePartition(topic, partid, offset)
        if err != nil { return err }
        startOffset = offset
      }
      log.Info("Starting partition consumer", "topic", topic, "partition", partid, "offset", offset, "startOffset", startOffset)
      go func(pc sarama.PartitionConsumer, startOffset int64, partid int32, i int, topic string) {
        var partBlacklist map[int64]struct{}
        if topicBlacklist, ok := kc.blacklist[topic]; ok {
          if pbl, ok := topicBlacklist[partid]; ok {
            partBlacklist = pbl
          } else {
            partBlacklist = make(map[int64]struct{})
          }
        } else {
          partBlacklist = make(map[int64]struct{})
        }
        kc.shutdownWg.Add(1)
        defer kc.shutdownWg.Done()
        dl.AddScale(i, startOffset)
        var once sync.Once
        warm := false
        meter := metrics.NewMinorMeter(fmt.Sprintf("streams/kafka/%v/%v/meter", topic, partid))
        lagGauge := metrics.NewMinorGauge(fmt.Sprintf("streams/kafka/%v/%v/lag", topic, partid))
        for input := range pc.Messages() {
          meter.Mark(1)
          lagGauge.Update(pc.HighWaterMarkOffset() - input.Offset)
          if !warm && input.Offset >= startOffset {
            // Once we're caught up with the startup offsets, wait until the
            // other partition consumers are too before continuing.
            warmupWg.Done()
            warmupWg.Wait()
            warm = true
          }
          if kc.ready != nil {
            if !kc.isReorg {
              dl.Step(i)
            }
            if pc.HighWaterMarkOffset() - input.Offset <= 1 {
              // Once we're caught up with the high watermark, let the ready
              // channel know
              once.Do(func() {
                log.Debug("Partition ready", "topic", input.Topic, "partition", input.Partition)
                dl.Done(i)
                readyWg.Done()
              })
            }
          }
          if _, blacklisted := partBlacklist[input.Offset]; !blacklisted {
            messages <- input
          }
        }
      }(pc, startOffset, partid, len(partitionConsumers), topic)
      partitionConsumers = append(partitionConsumers, pc)
    }
  }
  var readych chan time.Time
  readyWaiter := func() <-chan time.Time  {
    // If readych isn't ready to receive, use it as the sender, eliminating any
    // possibility that this case will trigger, and avoiding the creation of
    // any timer objects that will have to get cleaned up when we don't need
    // them.
    if readych == nil { return readych }
    // Only if the readych is ready to receive should this return a timer, as
    // the timer will have to be created, run its course, and get cleaned up,
    // which adds overhead
    return time.After(time.Second)
  }
  go func(wg, reorgWg *sync.WaitGroup) {
    // Wait until all partition consumers are up to the high water mark and alert the ready channel
    wg.Wait()
    log.Debug("Partitions ready")
    readych = make(chan time.Time)
    <-readych
    log.Debug("Ready")
    readych = nil
    reorgWg.Wait()
    log.Debug("Reorg waiter ready")
    kc.ready <- struct{}{}
    log.Debug("Ready sent")
    kc.ready = nil
    dl.Close()
    delivery.Ready()
  }(&readyWg, &reorgWg)
  go func() {
    hbGauge := metrics.NewMajorGauge("streams/kafka/heartbeat")
    hbGauge.Update(1)
    hbTicker := time.NewTicker(time.Minute)
    for {
      select {
      case input, ok := <-messages:
        if !ok {
          return
        }
        msg := &KafkaResumptionMessage{input}
        if len(msg.Key()) == 0 {
          // Update the heartbeat gauge to 1 (received heartbeat) and reset the
          // ticker to go off in one minute
          hbGauge.Update(1)
          hbTicker.Reset(time.Minute)
          continue
        }
        switch delivery.MessageType(msg.Key()[0]) {
        case delivery.ReorgType:
          if !kc.isReorg{
            rekc := &KafkaConsumer{
              omp: kc.omp,
              quit: make(chan struct{}),
              ready: make(chan struct{}),
              brokerURL: kc.brokerURL,
              topics: []string{fmt.Sprintf("%s-re-%x",  kc.defaultTopic, msg.Key()[1:])},
              resumption: make(map[string]int64),
              client: kc.client,
              isReorg: true,
            }
            rekc.Start()
            <-rekc.Ready()
            rekc.Close()
          } else {
            reorgWg.Add(1)
            if err := kc.omp.ProcessMessage(msg); err != nil {
              log.Error("Error processing input:", "err", err, "key", input.Key, "msg", input.Value, "part", input.Partition, "offset", input.Offset)
            }
          }
        case delivery.PingType:
          heartbeats <- types.BytesToHash(msg.Key()[1:])
        case delivery.ReorgCompleteType:
          reorgWg.Done()
        default:
          if err := kc.omp.ProcessMessage(msg); err != nil {
            log.Error("Error processing input:", "err", err, "key", input.Key, "msg", input.Value, "part", input.Partition, "offset", input.Offset)
          }
        }
      case <-dl.Reset(5000 * time.Millisecond):
        // Reset the drumline if we got no messages at all in a 5 second
        // period. This will be a no-op if the drumline is closed. Resetting if
        // we don't get any messages for five second should be safe, as the
        // purpose of the drumline here is to ensure no single partition gets
        // too far ahead of the others; if there are no messages being produced
        // by any of them, either there are no messages at all, or there are no
        // messages because the drumline has gotten streteched too far apart in
        // the course of normal operation, and it should be reset.
        log.Debug("Drumline reset")
      case v := <- readyWaiter():
        // readyWaiter() will wait 1 second if readych is ready to receive ,
        // which will trigger a message to get sent on kc.ready. This
        // should only trigger when we are totally caught up processing all the
        // messages currently available from Kafka. Before we reach the high
        // watermark and after this has triggered once, readyWaiter() will be a
        // nil channel with no significant resource consumption.
        select {
        case readych <- v:
        default:
        }
      case <-kc.quit:
        go func() {
          for _, c := range partitionConsumers {
            c.Close()
          }
          kc.shutdownWg.Wait()
          close(messages)
        }()
      case <-hbTicker.C:
        hbGauge.Update(0)
      }
    }
  }()
  go func() {
    for {
      select {
      case h := <-heartbeats:
        kc.pt.update(h)
      }
    }
  }()
  return nil
}
func (kc *KafkaConsumer) ProducerCount(d time.Duration) uint {
  return kc.pt.ProducerCount(d)
}

func (kc *KafkaConsumer) Ready() <-chan struct{} {
  if kc.ready != nil { return kc.ready }
  r := make(chan struct{}, 1)
  r <- struct{}{}
  return r
}
func (kc *KafkaConsumer) Subscribe(ch interface{}) types.Subscription {
  return kc.omp.Subscribe(ch)
}
func (kc *KafkaConsumer) SubscribeReorg(ch chan<- map[int64]types.Hash) types.Subscription {
  return kc.omp.SubscribeReorg(ch)
}
func (kc *KafkaConsumer) Close() {
  kc.quit <- struct{}{}
  kc.shutdownWg.Wait()
}
func (kc *KafkaConsumer) WhyNotReady(hash types.Hash) string {
  return kc.omp.WhyNotReady(hash)
}

func (kc *KafkaConsumer) Waiter() waiter.Waiter {
  if kc.waiter == nil {
    kc.waiter = waiter.NewOmpWaiter(kc.omp)
  }
  return kc.waiter
}