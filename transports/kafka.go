package transports

import (
  "fmt"
  "math/big"
  "github.com/Shopify/sarama"
  "github.com/openrelayxyz/cardinal-types"
  "github.com/openrelayxyz/cardinal-streams/delivery"
  log "github.com/inconshreveable/log15"
  coreLog "log"
  "net/url"
  "strings"
  "strconv"
  "time"
  "os"
)

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
    config.Producer.Retry.Max = 10000000
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
  brokers, config := ParseKafkaURL(brokerURL)
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
  done := func() { kp.reorgTopic = oldReorgTopic }
  if err := CreateTopicIfDoesNotExist(kp.brokerURL, kp.reorgTopic, -1, nil); err != nil {
    done()
    return func() {}, err
  }
  if err := kp.emit(kp.defaultTopic, msg); err != nil {
    done()
    return func(){}, err
  }
  return done, nil
}
