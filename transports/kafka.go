package transports

import (
  "fmt"
  "math/big"
  "github.com/Shopify/sarama"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-types/metrics"
  "github.com/openrelayxyz/cardinal-streams/delivery"
  "github.com/openrelayxyz/drumline"
  log "github.com/inconshreveable/log15"
  coreLog "log"
  "net/url"
  "regexp"
  "strings"
  "strconv"
  "sync"
  "time"
  "os"
)

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
  parsedURL, _ := url.Parse("kafka://" + brokerURL)
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
}

func NewKafkaProducer(brokerURL, defaultTopic string, schema map[string]string) (Producer, error) {
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
  return &KafkaProducer{dp: dp, producer: producer, reorgTopic: "", defaultTopic: defaultTopic, brokerURL: brokerURL}, nil
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
  msgs, err := kp.dp.AddBlock(number, hash, parentHash, weight, updates, deletes, batches)
  if err != nil { return err }
  return kp.emitBundle(msgs)
}
func (kp *KafkaProducer) SendBatch(batchid types.Hash, delete []string, update map[string][]byte) error {
  msgs, err := kp.dp.SendBatch(batchid, delete, update)
  if err != nil { return err }
  return kp.emitBundle(msgs)
}
func (kp *KafkaProducer) Reorg(number int64, hash types.Hash) (func(), error) {
  msg := kp.dp.Reorg(number, hash)
  oldReorgTopic := kp.reorgTopic
  kp.reorgTopic = fmt.Sprintf("%s-re-%x",  kp.defaultTopic, hash.Bytes())
  done := func() {
    kp.emit(kp.reorgTopic, kp.dp.ReorgDone(number, hash))
    kp.reorgTopic = oldReorgTopic
  }
  if err := CreateTopicIfDoesNotExist(kp.brokerURL, kp.reorgTopic, 1, nil); err != nil {
    done()
    return func() {}, err
  }
  if err := kp.emit(kp.defaultTopic, msg); err != nil {
    done()
    return func(){}, err
  }
  kp.emit(kp.reorgTopic, msg)
  return done, nil
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
  hwm := consumer.HighWaterMarks()
  var highestNumber int64
  for _, partid := range partitions {
    offset := hwm[kp.defaultTopic][partid]
    if offset == 0 { continue }
    offset -= 100
    if offset < 0 { offset = sarama.OffsetOldest }
    pc, err := consumer.ConsumePartition(kp.defaultTopic, partid, offset)
    if err != nil {
      pc, err = consumer.ConsumePartition(kp.defaultTopic, partid, sarama.OffsetOldest)
      if err != nil { return 0, err }
    }
    for input := range pc.Messages() {
      if pc.HighWaterMarkOffset() - input.Offset <= 1 {
        pc.AsyncClose()
      }
      if delivery.MessageType(input.Key[0]) == delivery.BatchMsgType {
        b, err := delivery.UnmarshalBatch(input.Value)
        if err != nil { continue }
        if b.Number > highestNumber { highestNumber = b.Number }
      }
    }
  }
  return highestNumber, nil
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
}

func kafkaConsumerWithOMP(omp *delivery.OrderedMessageProcessor, brokerURL, defaultTopic string, topics []string, resumption []byte, rollback int64, lastHash types.Hash) (Consumer, error) {
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
func NewKafkaConsumer(brokerURL, defaultTopic string, topics []string, resumption []byte, rollback, lastNumber int64, lastHash types.Hash, lastWeight *big.Int, reorgThreshold int64, trackedPrefixes []*regexp.Regexp, whitelist map[uint64]types.Hash) (Consumer, error) {
  omp, err := delivery.NewOrderedMessageProcessor(lastNumber, lastHash, lastWeight, reorgThreshold, trackedPrefixes, whitelist)
  if err != nil { return nil, err }
  return kafkaConsumerWithOMP(omp, brokerURL, defaultTopic, topics, resumption, rollback, lastHash)
}

func (kc *KafkaConsumer) Start() error {
  log.Debug("Starting kafka consumer")
  dl := drumline.NewDrumline(256)
  consumer, err := sarama.NewConsumerFromClient(kc.client)
  if err != nil { return err }
  var readyWg, warmupWg, reorgWg sync.WaitGroup
  messages := make(chan *sarama.ConsumerMessage, 512) // 512 is arbitrary. Worked for Flume, but may require tuning for Cardinal
  partitionConsumers := []sarama.PartitionConsumer{}
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
        kc.shutdownWg.Add(1)
        defer kc.shutdownWg.Done()
        dl.Add(i)
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
          messages <- input
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
    for {
      select {
      case input, ok := <-messages:
        if !ok {
          return
        }
        msg := &KafkaResumptionMessage{input}
        if len(msg.Key()) == 0 {
          log.Error("Error processing input: malformed message key")
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
      }
    }
  }()
  return nil
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
