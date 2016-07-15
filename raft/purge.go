package raft

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/golang/glog"
)

func (r *Service) calculatePurgeDelay() time.Duration {
	cfg := r.GetRaftConfig()
	if !cfg.ShouldPurgeRecords() {
		return math.MaxInt64
	}

	cfg.RLock()
	defer cfg.RUnlock()

	switch d := cfg.Internal().PurgeDuration; {
	case d < time.Minute:
		return time.Second
	case d < time.Hour:
		return time.Minute
	default:
		return time.Hour
	}
}

func (r *Service) startPurge(minIndex uint64) {
	go r.runPurge(minIndex)
}

func (r *Service) runPurge(minIndex uint64) {
	cfg := r.GetRaftConfig()
	if !cfg.ShouldPurgeRecords() {
		return
	}

	lastIndex, _, err := r.stor.GetLastIndex()
	if err != nil {
		glog.Errorf("Error calculating data to purge: %s", err)
		return
	}
	if lastIndex == 0 {
		return
	}

	lastEntry, err := r.stor.GetEntry(lastIndex)
	if err != nil {
		glog.Errorf("Error calculating data to purge: %s", err)
		return
	}
	if lastEntry.Type == PurgeRequest {
		// This prevents an infinite loop of purging and then sending purge requests
		glog.V(2).Info("Not calculating purge because last entry was a purge request")
		return
	}

	purgeIndex, err := r.stor.CalculateTruncate(
		uint64(cfg.MinPurgeRecords),
		cfg.Internal().PurgeDuration,
		minIndex)
	if err != nil {
		glog.Errorf("Error calculating data to purge: %s", err)
		return
	}

	if purgeIndex > 0 {
		glog.V(2).Infof("Proposing a database purge from index %d", purgeIndex)
		buf := make([]byte, binary.MaxVarintLen64)
		l := binary.PutUvarint(buf, purgeIndex)

		e := &common.Entry{
			Timestamp: time.Now(),
			Type:      PurgeRequest,
			Data:      buf[:l],
		}

		_, err = r.Propose(e)
		if err != nil {
			glog.Errorf("Error writing a purge request: %s", err)
		}
	}
}

func (r *Service) purgeData(e *common.Entry) {
	purgeIndex, c := binary.Uvarint(e.Data)
	if c <= 0 {
		glog.Error("Invalid purge data message received")
		return
	}

	go r.runDataPurge(purgeIndex)
}

func (r *Service) runDataPurge(purgeIndex uint64) {
	glog.Infof("Purging storage records before %d", purgeIndex)
	err := r.stor.TruncateBefore(purgeIndex)
	if err != nil {
		glog.Errorf("Error purging data: %s", err)
	}
}
