package raft

import (
	"fmt"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/protobufs"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
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
}

func (c *Config) shouldPurgeRecords() bool {
	return c.MinPurgeRecords > 0 || c.MinPurgeDuration > 0
}

func decodeRaftConfig(buf []byte) (*Config, error) {
	var pb protobufs.ConfigPb
	err := proto.Unmarshal(buf, &pb)
	if err != nil {
		return nil, err
	}

	rc := Config{}

	if pb.GetPurge() != nil {
		rc.MinPurgeRecords = pb.GetPurge().GetMinRecords()
		rc.MinPurgeDuration = time.Duration(pb.GetPurge().GetMinDuration()) * time.Millisecond
	}

	return &rc, nil
}

func (c *Config) encode() []byte {
	pb := protobufs.ConfigPb{}
	if c.MinPurgeDuration > 0 || c.MinPurgeRecords > 0 {
		pc := protobufs.PurgeConfig{}
		pc.MinDuration = proto.Int64(int64(c.MinPurgeDuration / time.Millisecond))
		pc.MinRecords = proto.Uint32(c.MinPurgeRecords)
		pb.Purge = &pc
	}

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

	r.setRaftConfig(cfg)
	r.stor.SetMetadata(RaftConfigKey, entry.Data)
	r.loopCommands <- UpdateRaftConfiguration
}
