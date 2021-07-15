package delivery

import (
  "encoding/binary"
  "log"
)

const (
  PayloadType = 0x0
  ExtendPayloadType = 0xFF
)

type Message interface {
  Key() []byte
  Value() []byte
  Items() ([]byte, []byte)
}

type Part struct {
  count uint             // How many more k/v pairs are expected
  data map[string][]byte // Map key to value
}

// Add collects the provided message under its prefix collection. If the
// expected number of messages with this prefix is reached (no more messages of
// this type are expected), this will retun True to indicate the payload may be
// complete if all other Parts are complete.
func (p *Part) Add(msg Message) bool {
  if _, ok := p.data[string(msg.Key())]; ok { return False }
  p.data[string(msg.Key())] = msg.Value()
  p.count--
  return p.count == 0
}

// Ready returns true when all packets of this part have been collected.
func (p *Part) Ready() bool {
  return p.count == 0
}

type Payload struct {
  parts map[string]Part // Map key prefixes to Parts
}

// Add collects the provided message under its prefix collection. If the
// expected number of messages with this prefix is reached (no more messages of
// this type are expected), this will retun True to indicate the payload may be
// complete if all other Parts are complete.
func (p *Payload) Add(msg Message) (bool, error) {
  prefix = string(msg.Key()[:33])
  part, ok := p.parts[prefix]
  if !ok { return false, ErrPrefixMissing }
  return part.Add(msg), nil
}

func (p *Payload) Ready() bool {
  for _, v := range p.parts {
    if !v.Ready() { return false }
  }
  return true
}

type Assembler struct {
  pending map[string]
  payloads [string]Payload
  prefixes [string]string   // Maps prefixes to payload id
  finished map[string]struct{}
  oldFinished map[string]struct{}
  subscribes map[byte]struct{}
}

func (a *Assembler) setupExpectedPayloads(payloaID string, value []byte) {
  body := value[:]
  for len(body) > 0 {
    prefix := string(body[:33])
    count := binary.BigEndian.Uint32(body[33:37])
    a.payloads[payloadID].parts[prefix] = Part{
      count: count,
      data: make(map[string][]byte),
    }
    a.prefixes[prefix] = payloadID
    body = body[37:]
  }
}

func (a *Assembler) HandleMessage(msg Message) []Payload {
  if handleMessage(msg) {
    // Return any ready payloads
  }
}

func (a *Assembler) handleMessage(msg Message) bool {
  key, value := msg.Items()
  if key[0] == 0x0 {
    payloadID := string(key[1:])
    if _, ok := a.payloads[payloadID]; ok { return false }
    a.payloads[payloadID] = Payload{
      parts: make(map[string]Part),
    }
    a.setupExpectedPayloads(payloadID, value)
    for k, msg := range a.pending {
      if !a.handleMessage(msg) {
        delete(a.pending, k)
      }
    }
    return true
  } else if key[0] == 0xFF {
    // Handle extension messages
    payloadID, ok := a.prefixes[string(key)]
    if !ok {
      // The prefix isn't known yet - queue this message to be handled when we
      // get the prefix.
      a.pending[string(key)] = msg
      return false
    }
    payload, ok := a.payloads[payloadID]
    if !ok {
      // This shouldn't happen - if the prefix is known they payload id should
      // always be known. If it does occur, we should
      panic("Payload missing for known prefix")
    }
    if _, ok := payload.parts[string(key)]; ok {
      // We've already handled this message, move on.
      return false
    }
    // Create a part with count 0 so we know we've handled this before
    payload.parts[string(key)] = Part{count: 0}
    a.setupExpectedPayloads(payloadID, value)
    for k, msg := range a.pending {
      if !a.handleMessage(msg) {
        delete(a.pending, k)
      }
    }
    return true
  } else {
    // Handle regular prefixed messages
    payloadID, ok := a.prefixes[string(key)]
    if !ok {
      // The prefix isn't known yet - queue this message to be handled when we
      // get the prefix.
      a.pending[string(key)] = msg
      return false
    }
    payload, ok := a.payloads[payloadID]
    if !ok {
      // This shouldn't happen - if the prefix is known they payload id should
      // always be known. If it does occur, we should
      panic("Payload missing for known prefix")
    }
    updated, err := payload.Add(msg)
    if err != nil { log.Printf("Error updating payload: %v", err.Error()) }
    return updated
  }
}
