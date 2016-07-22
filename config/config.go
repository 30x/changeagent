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
	DefaultElectionTimeout  = "10s"
	defaultElectionDuration = 10 * time.Second
	// DefaultHeartbeatTimeout is the default heartbeat timeout. It may be
	// modified using UpdateRaftConfig
	DefaultHeartbeatTimeout  = "2s"
	defaultHeartbeatDuration = 2 * time.Second

	minHeartbeatTimeout = 100 * time.Millisecond
	minHeartbeatRatio   = 2
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
	latch *sync.RWMutex

	// PurgeDuration is parsed from MinPurgeDuration
	purgeDuration time.Duration
	// ElectionDuration is parsed from ElectionTimeout
	electionDuration time.Duration
	// HeartbeatDuration is parsed from HeartbeatTimeout
	heartbeatDuration time.Duration
	// minPurgeRecords comes straight from YAML
	minPurgeRecords uint32
	webHooks        []hooks.WebHook
}

// StoredState is the structure that is read from and written to YAML
type StoredState struct {
	MinPurgeRecords  uint32          `yaml:"minPurgeRecords"`
	MinPurgeDuration string          `yaml:"minPurgeDuration"`
	ElectionTimeout  string          `yaml:"electionTimeout"`
	HeartbeatTimeout string          `yaml:"heartbeatTimeout"`
	WebHooks         []hooks.WebHook `yaml:"webHooks,omitempty"`
}

/*
MinPurgeRecords defines the minimum number of records that must be
retained on a purge. Default is zero, which means no purging.
*/
func (c *State) MinPurgeRecords() uint32 {
	c.RLock()
	defer c.RUnlock()
	return c.minPurgeRecords
}

/*
SetMinPurgeRecords updates MinPurgeRecords in a thread-safe way.
*/
func (c *State) SetMinPurgeRecords(v uint32) {
	c.Lock()
	c.minPurgeRecords = v
	c.Unlock()
}

/*
MinPurgeDuration defines the minimum amount of time that a record must
remain on the change list before being purged. Default is zero, which
no purging.
*/
func (c *State) MinPurgeDuration() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.purgeDuration
}

/*
SetMinPurgeDuration updates MinPurgeDuration in a thread-safe way.
*/
func (c *State) SetMinPurgeDuration(d time.Duration) {
	c.Lock()
	c.purgeDuration = d
	c.Unlock()
}

/*
ElectionTimeout is the amount of time a node will wait once it has heard
from the current leader before it declares itself a candidate.
It must always be a small multiple of HeartbeatTimeout.
*/
func (c *State) ElectionTimeout() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.electionDuration
}

/*
SetElectionTimeout updates ElectionTimeout in a thread-safe way.
*/
func (c *State) SetElectionTimeout(d time.Duration) {
	c.Lock()
	c.electionDuration = d
	c.Unlock()
}

/*
HeartbeatTimeout is the amount of time between heartbeat messages from the
leader to other nodes.
*/
func (c *State) HeartbeatTimeout() time.Duration {
	c.RLock()
	defer c.RUnlock()
	return c.heartbeatDuration
}

/*
SetHeartbeatTimeout updates HeartbeatTimeout in a thread-safe way.
*/
func (c *State) SetHeartbeatTimeout(d time.Duration) {
	c.Lock()
	c.heartbeatDuration = d
	c.Unlock()
}

/*
Timeouts is a quick way to get the election and heartbeat timeouts in
one call. The first one returned is the heartbeat timeout, then election
timeout.
*/
func (c *State) Timeouts() (time.Duration, time.Duration) {
	c.RLock()
	defer c.RUnlock()
	return c.heartbeatDuration, c.electionDuration
}

/*
WebHooks are a list of hooks that we might invoke before persisting a
change
*/
func (c *State) WebHooks() []hooks.WebHook {
	c.RLock()
	defer c.RUnlock()
	return c.webHooks
}

/*
SetWebHooks updates the web hooks in a thread-safe way.
*/
func (c *State) SetWebHooks(h []hooks.WebHook) {
	c.Lock()
	c.webHooks = h
	c.Unlock()
}

/*
GetDefaultConfig should be used as the basis for any configuration changes.
*/
func GetDefaultConfig() *State {
	cfg := new(State)
	cfg.latch = &sync.RWMutex{}

	cfg.Lock()
	cfg.electionDuration = defaultElectionDuration
	cfg.heartbeatDuration = defaultHeartbeatDuration
	cfg.Unlock()
	return cfg
}

/*
Load replaces the current configuration from a bunch of YAML.
*/
func (c *State) Load(buf []byte) error {
	var stored StoredState
	err := yaml.Unmarshal(buf, &stored)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()

	if stored.MinPurgeDuration != "" {
		c.purgeDuration, err = time.ParseDuration(stored.MinPurgeDuration)
		if err != nil {
			return fmt.Errorf("Error parsing minPurgeDuration: %s", err)
		}
	}
	if stored.ElectionTimeout != "" {
		c.electionDuration, err = time.ParseDuration(stored.ElectionTimeout)
		if err != nil {
			return fmt.Errorf("Error parsing electionTimeout: %s", err)
		}
	}
	if stored.HeartbeatTimeout != "" {
		c.heartbeatDuration, err = time.ParseDuration(stored.HeartbeatTimeout)
		if err != nil {
			return fmt.Errorf("Error parsing heartbeatTimeout: %s", err)
		}
	}

	c.minPurgeRecords = stored.MinPurgeRecords
	c.webHooks = stored.WebHooks

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

	stored := StoredState{
		MinPurgeRecords:  c.minPurgeRecords,
		ElectionTimeout:  fmt.Sprintf("%s", c.electionDuration),
		HeartbeatTimeout: fmt.Sprintf("%s", c.heartbeatDuration),
		MinPurgeDuration: fmt.Sprintf("%s", c.purgeDuration),
		WebHooks:         c.webHooks,
	}
	return yaml.Marshal(&stored)
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
	return c.minPurgeRecords > 0 && c.purgeDuration > 0
}

/*
Validate parses some of the strings in the configuration and it also
returns an error if basic parameters are not met.
*/
func (c *State) Validate() error {
	if c.heartbeatDuration < minHeartbeatTimeout {
		return fmt.Errorf("Heartbeat timeout must be at least %s", minHeartbeatTimeout)
	}
	if c.electionDuration < (c.heartbeatDuration * minHeartbeatRatio) {
		return fmt.Errorf("Election timeout %s must be at least %s",
			c.electionDuration, c.heartbeatDuration*minHeartbeatRatio)
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
