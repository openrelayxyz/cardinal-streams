package utils

import (
	"github.com/Shopify/sarama"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"strings"
)

// TopicConsumer consumes all of the partitions on a
type TopicConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Close() error
}

type topicConsumer struct {
	messages chan *sarama.ConsumerMessage
	partitionConsumers []sarama.PartitionConsumer
	consumer sarama.Consumer
}

func NewTopicConsumer(brokerURL, topic string, buffer int) (*topicConsumer, error) {
	tc := &topicConsumer{}
	brokers, config := transports.ParseKafkaURL(strings.TrimPrefix(brokerURL, "kafka://"))
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	tc.consumer, err = sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	tc.messages = make(chan *sarama.ConsumerMessage, buffer) // 512 is arbitrary. Worked for Flume, but may require tuning for Cardinal
	partitions, err := tc.consumer.Partitions(topic)
	tc.partitionConsumers = make([]sarama.PartitionConsumer, len(partitions))
	if err != nil { return nil, err }
	for i, partid := range partitions {
		pc, err := tc.consumer.ConsumePartition(topic, partid, sarama.OffsetNewest)
		if err != nil { return nil, err }
		go func(pc sarama.PartitionConsumer, i int) {
			for input := range pc.Messages() {
				tc.messages <- input
			}
		}(pc, len(tc.partitionConsumers))
		tc.partitionConsumers[i] = pc
	}
	return tc, nil
}

func (tc *topicConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return tc.messages
}

func (tc *topicConsumer) Close() error {
	for _, pc := range tc.partitionConsumers {
		if err := pc.Close(); err != nil { return err }
	}
	return tc.consumer.Close()
}
