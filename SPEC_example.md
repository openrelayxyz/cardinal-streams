## Worked Example

Given the Cardinal Streams [Spec](./SPEC.md), suppose you were adding a block
with the following parameters:

* Number: 1337
* Hash: 0xdeadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff
* Parent: 0x0000000000000000000000000000000000000000000000000000000000000000
* Weight: 0x2674
* Updates: {
    "a/b": 0x88,
    "q/17": 0x1234,
    "q/18": 0x5678,
    "x/19": 0x9999
  }
* Deletes: ["b/c"]
* Subbatches: {"b/s": 0x0000000000000000000000000000000000000000000000000000000000000001}
* Subbatch details:
  * Updates: {
    "b/s/5": 0xabcd
  }
  * Deletes: ["b/s/4"]

And a producer configured with the following prefixes:
* a/
* b/
* q/

This should produce the following messages:

## Message 0
Key: `$PrefixByte.$BlockHash` -> 0x00deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff
Value: Avro encoding of
```
Batch:
{
  "num": 1337,
  "weight": 0x2674,
  "parent": 0x0000000000000000000000000000000000000000000000000000000000000000,
  "updates": {
    "a/": {"count": 1},
    "b/": {"count": 1},
    "q/": {"count": 2},
    "b/s": {"subbatch": 0x0000000000000000000000000000000000000000000000000000000000000001},
    "x/19": {"value": 0x9999}
  }
}
```
## Message 1
(BatchMsg "a/b" -> 0x88)
Key: `$PrefixByte.$BlockHash.$Key` -> 0x03deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff612f62
Value: 0x88

## Message 2
(BatchMsg "q/17" -> 0x1234)
Key: `$PrefixByte.$BlockHash.$Key` -> 0x03deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff712f3137
Value: 0x1234

## Message 3
(BatchMsg "q/18" -> 0x5678)
Key: `$PrefixByte.$BlockHash.$Key` -> 0x03deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff712f3138
Value: 0x5678

## Message 4
(BatchDeleteMsg "b/c")
Key: `$PrefixByte.$BlockHash.$Key` -> 0x04deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff622f63
Value: 0x5678

## Message 5
(SubBatchHeader 0x0000000000000000000000000000000000000000000000000000000000000001)
Key: `$PrefixByte.$BlockHash.$BatchId` 0x01deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff0000000000000000000000000000000000000000000000000000000000000001
Value: Avro encoding of int 2

## Message 6
(SubBatchMsg 0)
Key: `$PrefixByte.$BlockHash.$BatchId.$Index` -> 0x02deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff000000000000000000000000000000000000000000000000000000000000000100
Value: Avro encoding of

```
SubBatchMsg
{
  "updates": {"b/s/5": 0xabcd}
}
```

## Message 7
(SubBatchMsg 1)
Key: `$PrefixByte.$BlockHash.$BatchId.$Index` -> 0x02deadbeef0123456789abcdef00000000000000fedcba987654321fffffffffff000000000000000000000000000000000000000000000000000000000000000102
Value: Avro encoding of

```
SubBatchMsg
{
  "delete": ["b/s/4"]
}
```
