Elements of a block:

* Headers
* Uncles list
* Transactions
  * Transaction Receipt Headers
  * Logs
* State Updates
  * Account updates
  * Account destructs
  * Storage updates
  * New code storage


For any given block:

header:
* 1 state delta
* t transactions
* t transaction receipts

transaction:
* <no sub-messages>

transaction receipt:
* k event logs

log:
* <no sub-messages>

state update:
* a account updates
* d account destructs
* s storage updates
* c code storage


Flume:
* Header
* Transactions
* Transaction Receipts
* Event Logs

Cardinal Replica:
* Header
* State update
* Accounts
* Destructs
* Storage
* Code

Proxy:
* Header
* Transactions
* Event logs

Full Node:
* Header
* Transactions
* Transaction Receipts
* Event Logs
* State Update
* Accounts
* Destructs
* Storage
* Code



## Header:

Key:
[
  MSG_TYPE, # 0x0  (1 byte)
  BLOCK_HASH,      (32 byte)
]

Value:

[
  TD,              (32 byte)
  TX_COUNT,        (4 byte)
  LOG_COUNT,       (4 byte)
  HEADER_RLP,      (Remaning bytes)
]

Topic: $PREFIX-header


## Transaction:

Key:

[
  MSG_TYPE, # 0x1 (1 byte)
  BLOCK_HASH,     (32 byte)
  TX_INDEX,       (4 byte)
]

Value:

[
  TX_HASH,        (32 bytes)
  TX_RLP,         (remaining bytes)
]

Topic: $PREFIX-tx


## Transaction Receipt:

Key:

[
  MSG_TYPE, # 0x2 (1 byte)
  BLOCK_HASH,     (32 byte)
  TX_INDEX,       (4 byte)
]

Value:

[
  RECEIPT_META_RLP,         (remaining bytes)
]

Topic: $PREFIX-receipt


## Log Message

Key:

[
  MSG_TYPE, # 0x3 (1 byte)
  BLOCK_HASH,     (32 byte)
  LOG_INDEX,      (4 byte)
]

Value:

[

]

Topic: $PREFIX-log

## State Delta

Key:

[
  MSG_TYPE, # 0xFF (1 byte)
  STATE_ROOT,     (32 byte)
]
