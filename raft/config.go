package raft

import (
	"fmt"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/protobufs"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
)

const (
	// DefaultElectionTimeout is the election timeout out of the box.
	// It may be modified using UpdateRaftConfig
	DefaultElectionTimeout = 10 * time.Second
	// DefaultHeartbeatTimeout is the default heartbeat timeout. It may be
	// modified using UpdateRaftConfig
	DefaultHeartbeatTimeout = 2 * time.Second

	minElectionTimeout = 100 * time.Millisecond
	minHeartbeatRatio  = 2
)

/*
Config describes configuration information about the raft service
that is shared around the cluster.
If either MinPurgeRecords OR MinPurgeDuration is set to greater than zero,
then the leader will send a purge request to the quorum every so often.
*/
type Config struct {
	// MinPurgeRecords defines the minimum number of records that must be
	// retained on a purge. Default is zero, which means no purging.
	MinPurgeRecords uint32
	// MinPurgeDuration defines the minimum amount of time that a record must
	// remain on the change list before being purged. Default is zero, which
	// no purging.
	MinPurgeDuration time.Duration
	// ElectionTimeout is the amount of time a node will wait once it has heard
	// from the current leader before it declares itself a candidate.
	// It must always be a small multiple of HeartbeatTimeout.
	ElectionTimeout time.Duration
	// HeartbeatTimeout is the amount of time between heartbeat messages from the
	// leader to other nodes.
	HeartbeatTimeout time.Duration
}

var defaultConfig = Config{
	ElectionTimeout:  DefaultElectionTimeout,
	HeartbeatTimeout: DefaultHeartbeatTimeout,
}

/*
GetDefaultConfig should be used as the basis for any configuration changes.
*/
func GetDefaultConfig() Config {
	return defaultConfig
}

func (c Config) shouldPurgeRecords() bool {
	return c.MinPurgeRecords > 0 && c.MinPurgeDuration > 0
}

func (c Config) validate() error {
	if c.ElectionTimeout <= minElectionTimeout {
		return fmt.Errorf("Heartbeat timeout must be at least %s", minElectionTimeout)
	}
	if c.ElectionTimeout < (c.HeartbeatTimeout * minHeartbeatRatio) {
		return fmt.Errorf("Election timeout %s must be at least %s",
			c.ElectionTimeout, c.HeartbeatTimeout*minHeartbeatRatio)
	}
	return nil
}

func decodeRaftConfig(buf []byte) (Config, error) {
	var pb protobufs.ConfigPb
	err := proto.Unmarshal(buf, &pb)
	if err != nil {
		return defaultConfig, err
	}

	rc := Config{}

	if pb.GetPurge() != nil {
		rc.MinPurgeRecords = pb.GetPurge().GetMinRecords()
		rc.MinPurgeDuration = time.Duration(pb.GetPurge().GetMinDuration()) * time.Millisecond
	}
	rc.HeartbeatTimeout = time.Duration(pb.GetHeartbeatTimeout())
	rc.ElectionTimeout = time.Duration(pb.GetElectionTimeout())

	return rc, nil
}

func (c Config) encode() []byte {
	pb := protobufs.ConfigPb{}
	if c.MinPurgeDuration > 0 || c.MinPurgeRecords > 0 {
		pc := protobufs.PurgeConfig{}
		pc.MinDuration = proto.Int64(int64(c.MinPurgeDuration / time.Millisecond))
		pc.MinRecords = proto.Uint32(c.MinPurgeRecords)
		pb.Purge = &pc
	}
	pb.HeartbeatTimeout = proto.Int64(int64(c.HeartbeatTimeout))
	pb.ElectionTimeout = proto.Int64(int64(c.ElectionTimeout))

	buf, err := proto.Marshal(&pb)
	if err != nil {
		panic(fmt.Sprintf("Error encoding Raft configuration: %s", err))
	}
	return buf
}

func (r *Service) applyRaftConfigChange(entry *common.Entry) {
	cfg, err := decodeRaftConfig(entry.Data)
	if err != nil {
		glog.Errorf("Received invalid raft configuration data: %s", err)
		return
	}
	err = cfg.validate()
	if err != nil {
		glog.Errorf("Received invalid raft configuration data: %s", err)
		return
	}

	r.setRaftConfig(cfg)
	r.stor.SetMetadata(RaftConfigKey, entry.Data)
}
