package delivery

import (
  "fmt"
)

var (
  ErrPrefixMissing = fmt.Errorf("prefix missing")
  ErrPrefixOverlap = fmt.Errorf("producers must not have overlapping prefixes")
  ErrUnknownBatch = fmt.Errorf("unknown batch")
  ErrPrefixConflict = fmt.Errorf("deletes, updates, and batches cannot have prefix conflicts")
)
