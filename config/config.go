package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/30x/changeagent/hooks"
)

const (
	// DefaultElectionTimeout is the election timeout out of the box.
	// It may be modified using UpdateRaftConfig
	DefaultElectionTimeout = "10s"
	// DefaultHeartbeatTimeout is the default heartbeat timeout. It may be
	// modified using UpdateRaftConfig
	DefaultHeartbeatTimeout = "2s"

	minElectionTimeout = 100 * time.Millisecond
	minHeartbeatRatio  = 2
)

/*
State describes configuration information about the raft service
that is shared around the cluster.
If either MinPurgeRecords OR MinPurgeDuration is set to greater than zero,
then the leader will send a purge request to the quorum every so often.
A State is also an RWMutex so that it can (and should) be locked and
unlocked on each go-around.
*/
type State struct {
	latch    *sync.RWMutex
	internal *InternalState

	// MinPurgeRecords defines the minimum number of records that must be
	// retained on a purge. Default is zero, which means no purging.
	MinPurgeRecords uint32 `yaml:"minPurgeRecords"`
	// MinPurgeDuration defines the minimum amount of time that a record must
	// remain on the change list before being purged. Default is zero, which
	// no purging.
	MinPurgeDuration string `yaml:"minPurgeDuration"`
	// ElectionTimeout is the amount of time a node will wait once it has heard
	// from the current leader before it declares itself a candidate.
	// It must always be a small multiple of HeartbeatTimeout.
	ElectionTimeout string `yaml:"electionTimeout"`
	// HeartbeatTimeout is the amount of time between heartbeat messages from the
	// leader to other nodes.
	HeartbeatTimeout string `yaml:"heartbeatTimeout"`
	// WebHooks are a list of hooks that we might invoke before persisting a
	// change
	WebHooks []hooks.WebHook `yaml:"webHooks,omitempty"`
}

/*
InternalState holds stuff that's not persisted in YAML.
*/
type InternalState struct {
	// PurgeDuration is parsed from MinPurgeDuration
	PurgeDuration time.Duration
	// ElectionDuration is parsed from ElectionTimeout
	ElectionDuration time.Duration
	// HeartbeatDuration is parsed from HeartbeatTimeout
	HeartbeatDuration time.Duration
}

/*
GetDefaultConfig should be used as the basis for any configuration changes.
*/
func GetDefaultConfig() *State {
	cfg := new(State)
	cfg.latch = &sync.RWMutex{}
	cfg.internal = &InternalState{}

	cfg.Lock()
	cfg.ElectionTimeout = DefaultElectionTimeout
	cfg.HeartbeatTimeout = DefaultHeartbeatTimeout
	cfg.Validate()
	cfg.Unlock()
	return cfg
}

/*
Internal returns another set of config params that aren't directly persisted
to YAML but parsed instead.
*/
func (c *State) Internal() *InternalState {
	return c.internal
}

/*
Load replaces the current configuration from a bunch of YAML.
*/
func (c *State) Load(buf []byte) error {
	c.Lock()
	defer c.Unlock()

	c.WebHooks = nil

	err := yaml.Unmarshal(buf, c)
	if err != nil {
		return err
	}
	err = c.Validate()
	if err != nil {
		return err
	}
	return nil
}

/*
LoadFile loads configuration from a file.
*/
func (c *State) LoadFile(fileName string) error {
	cf, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer cf.Close()

	buf, err := ioutil.ReadAll(cf)
	if err != nil {
		return err
	}

	return c.Load(buf)
}

/*
Store returns the encoded configuration as a byte slice.
*/
func (c *State) Store() ([]byte, error) {
	c.RLock()
	defer c.RUnlock()

	return yaml.Marshal(c)
}

/*
StoreFile writes the configuration to a file.
*/
func (c *State) StoreFile(fileName string) error {
	buf, err := c.Store()
	if err != nil {
		return err
	}
	cf, err := os.OpenFile(fileName, syscall.O_WRONLY|syscall.O_CREAT, 0666)
	if err != nil {
		return err
	}
	defer cf.Close()

	_, err = cf.Write(buf)
	return err
}

/*
ShouldPurgeRecords is a convenience that lets us know if both parameters are
set to cause automatic purging to happen.
*/
func (c *State) ShouldPurgeRecords() bool {
	c.RLock()
	defer c.RUnlock()
	return c.MinPurgeRecords > 0 && c.internal.PurgeDuration > 0
}

/*
Validate parses some of the strings in the configuration and it also
returns an error if basic parameters are not met.
*/
func (c *State) Validate() error {
	var err error
	if c.MinPurgeDuration != "" {
		c.internal.PurgeDuration, err = time.ParseDuration(c.MinPurgeDuration)
		if err != nil {
			return fmt.Errorf("Error parsing minPurgeDuration: %s", err)
		}
	}
	if c.ElectionTimeout != "" {
		c.internal.ElectionDuration, err = time.ParseDuration(c.ElectionTimeout)
		if err != nil {
			return fmt.Errorf("Error parsing electionTimeout: %s", err)
		}
	}
	if c.HeartbeatTimeout != "" {
		c.internal.HeartbeatDuration, err = time.ParseDuration(c.HeartbeatTimeout)
		if err != nil {
			return fmt.Errorf("Error parsing heartbeatTimeout: %s", err)
		}
	}

	if c.internal.ElectionDuration <= minElectionTimeout {
		return fmt.Errorf("Heartbeat timeout must be at least %s", minElectionTimeout)
	}
	if c.internal.ElectionDuration < (c.internal.HeartbeatDuration * minHeartbeatRatio) {
		return fmt.Errorf("Election timeout %s must be at least %s",
			c.internal.ElectionDuration, c.internal.HeartbeatDuration*minHeartbeatRatio)
	}
	return nil
}

// Lock locks the internal latch just like a Mutex
func (c *State) Lock() {
	c.latch.Lock()
}

// Unlock unlocks the internal latch just like a Mutex
func (c *State) Unlock() {
	c.latch.Unlock()
}

// RLock locks the internal latch just like an RWMutex
func (c *State) RLock() {
	c.latch.RLock()
}

// RUnlock unlocks the internal latch just like an RWMutex
func (c *State) RUnlock() {
	c.latch.RUnlock()
}
