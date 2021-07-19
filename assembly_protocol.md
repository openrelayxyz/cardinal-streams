The reassembly protocol is designed to solve data replication problems common
in blockchain replication. It is built to solve the following problems:

* We will have multiple producers of information, for redundancy and
  resiliency.
* We will have different blockchain clients producing the same information. It
  is important that these clients produce the same information in the same way
  so that consumers produce the same results.
* Messages are too large to transmit efficiently in a single packet, and must
  be broken up and reassembled.
* When broken up, no assumptions can be made about the delivery order of
  packets, or the number of times a given packet will be delivered (except that
  each packet should be delivered at least once).
* Different consumers will want different subsets of the main message. Rather
  than having to get the entire message, consumers should be able to subscribe
  to the portions they are interested in.




Assembly:

```
queue = []
payloads = {}
prefixes = {}
finished = set()
oldFinished = set()

def process_message(msg):
    prefix = msg.key[:33]
    try:
        payload_id = prefixes[prefix]
    except KeyError:
        queue.append(msg)
    payload = payloads[payload_id]
    prefix_data = payload[prefix]
    if msg.key not in prefix_data["parts"]:
        prefix_data["count"] -= 1
        prefix_data["parts"][msg.key] = msg.value


for msg in stream:
    if msg.key[0] == 0x0:
        if msg.key[1:] in payloads:
            continue
        payload = payloads.setdefault(msg.key[1:], {})
        body = msg.value
        while body:
            prefix = body[:33]
            count = parseInt(body[33:37])
            prefixes[prefix] = msg.key[1:]
            prefix_data = payload.setdefault(prefix, {})
            prefix_data["count"] = count
            prefix_data["parts"] = {}
            body = body[:37]
        for msg in queue:
            process_message(msg)
    elif msg.key[0] = 0xFF:
        prefix = msg.key[:33]
        try:
            payload_id = prefixes[prefix]
        except KeyError:
            queue.append(msg)
        payload = payloads[payload_id]
        prefix_data = payload[prefix]
        prefix_data["count"] -= 1
        body = msg.value
        while body:
            prefix = body[:33]
            count = parseInt(body[33:37])
            prefixes[prefix] = msg.key[1:]
            prefix_data = payload.setdefault(prefix, {})
            prefix_data["count"] = count
            prefix_data["parts"] = {}
            body = body[:37]
        for msg in queue:
            process_message(msg)
    else:
        process_message(msg)
    for k, v in payload
        if v["count"] > 0:
            break
    else:
        emit(payload)
        finished.add(prefixes[prefix])
        if len(finished) > reorgThreshold:
            for payload_id in oldFinished:
                for prefix in payloads[payload_id]:
                    delete(prefixes[prefix])
                delete payloads[payload_id]
            oldFinished = finished
            finished = set()
```
