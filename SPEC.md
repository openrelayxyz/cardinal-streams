# Cardinal Streams Protocol Specification v1.0.0

Cardinal Streams is designed to facilitate the transmission of blockchain
related data over message brokers. It is designed to support:

* Multiple redundant producers
* Multiple topics carrying different types of block data
* Multiple partitions for each topic

Which collectively imply:

* Duplicate message delivery
* Out-of-order message delivery
* Consumers interested in only a subset of topics

The Cardinal Streams Protocol is broken out into three layers:

* *The Message Layer*: Simple packets consisting of partition keys and values, which can be reassembled into larger payloads representing a particular block.
* *The Transport Layer*: Specifications for sending messages across a specific kind of message broker.
* *The Application Layer*: Specifies the data schema for a particular application, and transport layer configurations for that application.

This specification deals primarily with the Message Layer and specifies a few
requirements all Application Layers and Transport Layers must implement.

## Application Layer Requirements

Applications must identify the message Prefixes for values that can potentially
be included payloads. This allows Producers to specify the number of messages
to expect for each Prefix, and allows Consumers to specify which prefixes they
are interested in tracking.

Applications may additionally provide a mapping of which message prefixes
should be transmitted over which Topics, depending on the nature of the
supported Transports.

## Messages

All messages consist of a partition key and a value. When complex data
encodings are required by the streams protocol, the Avro data encoding will be
used, with the Avro schema specified in the spec. Message payloads specified
by an Application Layer may used alternative encodings convenient to the
application.


### Message Payloads

Message Payloads are collections of information pertaining to a particular
block. Every message payload must include:

* *BlockHash*: A 32 byte identifier unique to this message payload.
* *Number*: A 64 bit unsigned integer indicating the sequence number of this payload.
* *Parent*: The $BlockHash of the block preceding this one.
* *Weight*: A 256 bit unsigned indicator indicating the weight of this payload. If this is not meaningful for a given application, it can duplicate $Number.

Note that given the nature of blockchains, it is expected that a payload's
$Number will not always be unique, but there can only be one instance of a
given $Number in a single chain. The $BlockHash is expected to be unique, and
except when bootstrapping the first block of a stream, every payload's $Parent
should have been transmitted to the stream prior to the payload that references
it as $Parent.

Most payloads will also provide Updates - a set of values to be changed by the
payload.

A payload will be split up into numerous messages to be emitted across the
Transport Layer, with one message describing properties needed for reassembling
the original payload.

#### Batch
* *Prefix Byte:* `\x00`
* *Key*: `$PrefixByte.$BlockHash`
* *Value*:

Avro Encoding:
```
{
  "type": "record",
  "name": "batch",
  "avro.codec": "snappy",
  "namespace": "cloud.rivet.cardinal",
  "fields": [
    {"name": "num", "type": "long"},
    {"name": "weight", "type": "bytes"},
    {"name": "parent", "type": {"name": "par", "type": "fixed", "size": 32}},
    {
      "name": "updates",
      "type": {"name": "upd", "type": "map", "values": {
        "type": "record",
        "name": "update",
        "fields": [
          {"name": "value", "type": "bytes"},
          {"name": "count", "type": "int"},
          {"name": "delete", "type": "boolean"},
          {"name": "subbatch", "type": {"name": "subb", "type": "fixed", "size": 32}}
        ]
      }}
    }
  ]
}
```

##### Producers

Producers should create one `Batch` message per payload according to the above
schema, where

* `num` = $Number
* `weight` = $Weight
* `parent` = $Parent

For the `updates` map, keys in the map will correspond to prefixes indicated by
the Application layer. The value will be a map containing one of:

* `value`: A raw value to set the specified key to
* `count`: The number of `SubBatchMsg`s prefixed by this Key.
* `delete`: If `true`, records with this prefix should be deleted.
* `subbatch`: A 32 byte identifier indicating that a `SubBatchMsg` will follow to describe a sub batch.

##### Consumers

When a consumer receives a `Batch` message, it should immediately track:

* Hash: The payload hash (indicated in the message Key)
* Number: The payload number
* Weight: The payload weight
* ParentHash: The payload's parent's hash
* Prefix Counters: The number of messages of each prefix yet to be collected
* Pending Batches: The set of subbatches not yet collected.

And should prepare to collect:
* `Values`: Key/value pairs, based on the data schema for the application. These may be new values or updates.
* `Deletes`: A set of values to be deleted.
* `PendingBatches`: A set of sub batches to be expected as a part of this batch.
* `SubBatchCount`: The number of `SubBatchMsg` messages remaining. This will initially be `0`, but may increase as `SubBatchHeaders` as received.

When all of the prefix counters have reached zero and all pending batches have
been received, and the subbatch message counter  the batch is complete and can be passed on for subsequent
processing.

**Note**: Consumers may choose to monitor only certain prefixes, and can ignore
prefixes containing data they do not need.

#### SubBatchHeader

Sub batches are a way for producers to send messages that may not be known at
the time of the initial batch messages, or may not comply with the normal data
schema.

* *Prefix Byte:* `\x01`
* *Key*: `$PrefixByte.$BlockHash.$BatchId`
* *Value*: `$ItemCount`

Where

* `$BlockHash`: must match the Batch block hash.
* `$BatchId`: Must correspond to a pending batch specified in the Block's Pending Batches entry.
* `$ItemCount`: Is an Avro encoded integer of the number of items in this batch.

##### Producers

Producers MUST send one SubBatchHeader message for each Pending Batch specified
in the original Batch message, even if the `$ItemCount` for that SubBatch ends
up being `0`.

#### Consumers

If this `$BlockHash.$BatchId` combination has been previously been received,
this message should be discarded.

Consumers should increment `SubBatchCount` by `$ItemCount` if the `BatchId`
matches one expected by `Pending Batches`. Consumers must also prepare to track
which messages for a given batch have been received, so that duplicate messages
can be discarded.

If any `SubBatchMsg` messages for a given batch were received before the batch
itself, they should have been queued. That queue should be processed upon
receipt of the `SubBatchHeader` message.

#### SubBatchMsg
* *Prefix Byte:* `\x02`
* *Key*: `$PrefixByte.$BlockHash.$BatchId.$Index`
* *Value*:
```
{
  "type": "record",
  "name": "update",
  "fields": [
    {"name": "delete", "type": {"name": "del", "type": "array", "items": "string"}},
    {"name": "updates", "type": {"name": "upd", "type": "map", "values": "bytes"}}
  ]
}
```

##### Producers

Producers must produce one `$SubBatchMsg` for each `$Index` from
`[0, $ItemCount)` for the $ItemCount included in the `SubBatchHeader` message
corresponding to this `$BatchId`.

Each message may include zero or more `delete` records, and zero or more
`update` records.

##### Consumers

If the `$BlockHash.$BatchId` matches a known Batch and the `$Index` of this
message has not previously been processed, this message should be processed. If
the `$SubBatchHeader` for this `$BatchId` has not been received, this message
must be queued for later processing. If the `$Index` for this
`$BlockHash.$BatchId` has previously been processed, this message must be
discarded.

Processing:

Consumers should add any `deletes` to the Batch's `Deletes` property and any
`updates` to the Batch's `Values` property. The count of records expected for
this `SubBatch` should be decremented, and the `$Index` should be marked as
processed.

#### BatchMsg
* *Prefix Byte:* `\x03`
* *Key*: `$PrefixByte.$BlockHash.$Key`
* *Value*: `$Value`

##### Producers

Producers must emit `$Count` unique `BatchMsg` or `BatchDeleteMsg` messages
with a `$Key` beginning with each prefix specified in the original Batch
Message. The `$Key` and `$Value` correspond to the application specific data
schema.

##### Consumers

If `$Key` is not present in the batch's `Value` property, it should be set to
`$Value` and the batch prefix beginning `$Key` should have its counter
decremented. If `$Key` is present in the batch's `Value` property, this message
should be discarded.

#### BatchDeleteMsg
* *Prefix Byte:* `\x04`
* *Key*: `$PrefixByte.$BlockHash.$Key`
* *Value*: Empty

##### Producers

Producers must emit `$Count` unique `BatchMsg` or `BatchDeleteMsg` messages
with a `$Key` beginning with each prefix specified in the original Batch
Message. The `$Key` and `$Value` correspond to the application specific data
schema.

##### Consumers

If `$Key` is not present in the batch's `Delete` property, it should be added
to the set and the batch prefix beginning `$Key` should have its counter
decremented. If `$Key` is present in the batch's `Delete` property, this
message should be discarded.

### Reorg Handling

Consumers should retain enough historic batches to be able to handle reorgs up
to an application defined `ReorgThreshold`. If a reorg occurs in excess of the
application defined `ReorgThreshold`, Reorg Messages should be emitted.

#### Reorg
* *Prefix Byte:* `\x05`
* *Key*: `$PrefixByte.$Hash`
* *Value*: `$Number`

`$Hash` indicates  the common ancestor of the previous chain and the new chain,
with `$Number` indicating block number of that common ancestor.

##### Producers

When an error exceeding the application defined `ReorgThreshold` occurs, the
producer must create a new channel specifically for this reorg, then emit a
`ReorgType` message indicating the number and hash of the common ancestor of
the reorg. The specifics of this channel will depend on the transport, but the
channel should guarantee in-order delivery so that the `ReorgComplete` message
is guaranteed to be delivered after all batches in the reorg.

Following the `Reorg` message, the producer should produce batches for all
blocks starting after the common ancestor, up to the block with a `Weight`
heavier than the heaviest block on the removed chain. The last message produced
on the reorg channel should be a `ReorgComplete` message.

##### Consumers

Upon receipt of the `Reorg` message, the consumer should revert to the common
ancestor block, then process messages on the reorg channel according to the
rules for their respective message types. Upon receiving a `ReorgComplete`
message, the consumer should cease processing messages from the `Reorg`
channel and return to normal processing.

#### ReorgComplete
* *Prefix Byte:* `\x06`
* *Key*: `$PrefixByte.$Hash`
* *Value*: `$Number`

Usage of the `ReorgComplete` message is described above with the `Reorg`
message.
