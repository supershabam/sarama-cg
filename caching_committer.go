package cg

import (
	"sync"
	"time"
)

// CachingCommitterConfig is required to create a new CachingCommitter.
type CachingCommitterConfig struct {
	Coordinator *Coordinator
	Duration    time.Duration
	Partition   int32
	Topic       string
}

// CachingCommitter is a helper for consumers that need to implement committing
// offsets.
type CachingCommitter struct {
	cfg     *CachingCommitterConfig
	last    time.Time
	highest int64
	m       sync.RWMutex
}

// NewCachingCommitter helps consumers implement their CommittOffset function by
// wrapping the Coordinator.CommitOffset function with a cache so we don't hit
// the database too frequently.
func NewCachingCommitter(cfg *CachingCommitterConfig) (*CachingCommitter, error) {
	highest, err := cfg.Coordinator.GetOffset(cfg.Topic, cfg.Partition)
	if err != nil {
		return nil, err
	}
	return &CachingCommitter{
		cfg:     cfg,
		highest: highest,
	}, nil
}

// CommitOffset writes the provided offset to the database solang as the offset
// is higher than what we've already committed and the time since our last commit
// is greater than the configured duration.
func (cc *CachingCommitter) CommitOffset(offset int64) error {
	now := time.Now()
	cc.m.RLock()
	if cc.highest > offset {
		cc.m.RUnlock()
		return nil
	}
	if cc.last.Add(cc.cfg.Duration).After(now) {
		cc.m.RUnlock()
		return nil
	}
	cc.m.RUnlock()
	cc.m.Lock()
	defer cc.m.Unlock()
	// re-run conditions now we have the write-lock to allow early exit.
	if cc.highest > offset {
		return nil
	}
	if cc.last.Add(cc.cfg.Duration).After(now) {
		return nil
	}
	err := cc.cfg.Coordinator.CommitOffset(cc.cfg.Topic, cc.cfg.Partition, offset)
	if err != nil {
		return err
	}
	cc.highest = offset
	cc.last = now
	return nil
}
