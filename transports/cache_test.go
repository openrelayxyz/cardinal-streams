package transports

import (
    lru "github.com/hashicorp/golang-lru"
    types "github.com/openrelayxyz/cardinal-types"
    "testing"
)

func TestCache(t *testing.T) {
    cache, _ := lru.New(10)
    x := types.HexToHash("0x10")
    y := types.HexToHash("0x10")
    cache.Add(x, struct{}{})
    if !cache.Contains(y) {
        t.Errorf("Should have matched")
    }
}
