# Websockets Transport for Cardinal Streams

Moving forward with Flume, we need to be able to get information predating
Kafka's retention period. This could also be useful with Cardinal EVM in the
event that we ever need to do an emergency recovery without an available topic.

## Muxing Producer

One thing that we'll want to consider is a muxing producer, where a Cardinal
streams producer can fire off one payload that gets sent to multiple
transports, so a single producer could support both Kafka and Websockets (for
example).

## Resumption

We don't want the WebSockets producer to try to retain old messages for ever so
that new services coming online can resume from websockets. Instead, we should
have an interface that services can provide, along the lines of:

```
type StreamsResumption interface {
  BlocksFrom(block uint64, hash types.Hash, prefixes []regexp.Regexp) (chan *delivery.PendingBatch, error)
}
```

Such that when someone subscribes with a WebSocket connection they can specify
the block number and hash (to allow for reorgs), and get a feed of
PendingBatches to get from that block up to current. The prefixes can be used
to limit to information this consumer cares about.

Note that if the provided hash does not match the producer's block hash at that
height, the producer should go back $ReorgThreshold blocks and start from
there, as otherwise there is no information to determine the common ancestor
between the consumer and the producer.

A system like Cardinal EVM's producer plugin would need to provide a
StreamsResumption interface when instantiating the WebsocketsProducer.

## Streaming Format

With WebSockets (as opposed to a message broker), there is little reason to
break batches up into many smaller chunks. Producers should send full
PendingBatches across the wire in a format to be determined (probably JSON).

WebsocketConsumers will receive these full batches, but should not assume
in-order or exactly-once delivery. WebsocketConsumers will likely want to take
advantage of the existing OrderedMessage processor.

## Other Transports

While websockets is the first targeted alternative transport, the principles
behind a StreamsResumption interface could potentially be used for other
transports, such as IPC sockets, 0MQ, or other point-to-point protocols.


## Connection Protocol

Cardinal Streams Websockets will probably be based on the JSON-RPC schema used
by standard Web3 tooling, most likely leveraging Cardinal RPC (which will need
to be extended to support Websockets and subscriptions). A subscription message
will likely look like:

```
{"jsonrpc": "2.0", "id": 1, "method": "cardinal_subscribe", "params": ["streams", "$BLOCK_NUMBER", "$BLOCK_HASH", ["$PREFIX1", "$PREFIX2", "..."]]}
```

This would respond with a subscription ID:

```
{"jsonrpc": "2.0", "id": 1, "result": "0x1"}
```

Followed by subscription messages:

```
{"jsonrpc":"2.0","method":"cardinal_subscribe","params":{"subscription":"0x1","result":{"type":"batch","num":"0x1234","hash":"0x4223c4952f080373e1aa43e5f57d9da336656da10f8e5d24915b76c4524a20fb","parent":"0xdb6924b3e1de63e41162c44f59e96e1c8770300df5113c91e204342b66af46fa","updates":{"k":"0x01234","k2":"0x5678"},"deletes":["k4","k5"],"batches":{"s/":"0xfe148cdf84f41663d66364e99942da27a03da28a7d6f9ef46f84d57c28828aed"}}}}
{"jsonrpc":"2.0","method":"cardinal_subscribe","params":{"subscription":"0x1","result":{"type":"subbatch","hash":"0x4223c4952f080373e1aa43e5f57d9da336656da10f8e5d24915b76c4524a20fb","id":"0xfe148cdf84f41663d66364e99942da27a03da28a7d6f9ef46f84d57c28828aed","updates":{"s/k":"0x01234","s/k2":"0x5678"},"deletes":[]}}}
```

Note that upon subscription, Cardinal Streams will invoke the provider's
StreamsResumption tool and begin sending blocks starting at $BLOCK_NUMBER (or
$BLOCK_NUMBER - $REORG_THRESHOLD if $BLOCK_HASH does not match), but the stream
may also include newly broadcast blocks interspersed. It is the consumer's
responsibility to track, deduplicate, and reorder these blocks.
