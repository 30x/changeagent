package raft

import (
	cryptoRand "crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/30x/changeagent/common"
	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/config"
	"github.com/30x/changeagent/hooks"
	"github.com/30x/changeagent/storage"
	"github.com/golang/glog"
)

/*
 * Keys for the metadata API -- each goes into the metadata collection in the storage API.
 * Make these hard-coded rather than "iota" because they go in a database!
 */
const (
	CurrentTermKey = "currentTerm"
	VotedForKey    = "votedFor"
	LocalIDKey     = "localID"
	ClusterIDKey   = "clusterID"
	LastAppliedKey = "lastApplied"
	NodeConfig     = "nodeConfig"
)

const (
	// MembershipChange denotes a special message type for membership changes.
	MembershipChange = -1
	// PurgeRequest denotes that the leader would like to propose purging all
	// records older than the specified index. Body is just a change number
	// encoded using a "varint".
	// -2 and -3 was used in an old version
	PurgeRequest = -4
	// ConfigChange denotes a new configuration file that describes various
	// parameters about the implementation
	ConfigChange = -5

	jsonContent = "application/json"
)

// State is the current state of the Raft implementation.
type State int32

//go:generate stringer -type State .

/*
 * State of this particular node.
 */
const (
	Follower State = iota
	Candidate
	Leader
	Standalone
	Stopping
	Stopped
)

// MembershipChangeMode is the state of the current membership change process
type MembershipChangeMode int32

//go:generate stringer -type MembershipChangeMode .

/*
 * State of the current membership change process
 */
const (
	Stable MembershipChangeMode = iota
	ProposedJointConsensus
	ProposedFinalConsensus
)

// LoopCommand is used to send configuration changes to the main loop
type LoopCommand int32

//go:generate stringer -type LoopCommand .

/*
 * Commands to send to the main loop.
 */
const (
	UpdateNodeConfiguration LoopCommand = iota
	JoinAsFollower
	JoinAsCandidate
	UpdateRaftConfiguration
)

/*
Service is an instance of code that implements the Raft protocol.
It relies on the Storage, Discovery, and Communication services to do
its work, and invokes the StateMachine when changes are committed.
*/
type Service struct {
	id                   common.NodeID
	clusterID            uint64
	leaderID             uint64
	localAddress         atomic.Value
	state                int32
	comm                 communication.Communication
	nodeConfig           NodeList
	configFile           string
	raftConfig           *config.State
	stor                 storage.Storage
	loopCommands         chan LoopCommand
	stopChan             chan chan bool
	voteCommands         chan voteCommand
	appendCommands       chan appendCommand
	proposals            chan proposalCommand
	statusInquiries      chan chan<- ProtocolStatus
	latch                sync.Mutex
	followerOnly         bool
	currentTerm          uint64
	commitIndex          uint64
	lastApplied          uint64
	lastIndex            uint64
	lastTerm             uint64
	membershipChangeMode int32
	appliedTracker       *ChangeTracker
	stateMachine         StateMachine
}

/*
ProtocolStatus returns some of the diagnostic information from the raft engine.
*/
type ProtocolStatus struct {
	// If this node is the leader, a map of the indices of each peer.
	// Otherwise nil.
	PeerIndices *map[common.NodeID]uint64
}

type voteCommand struct {
	vr communication.VoteRequest
	rc chan communication.VoteResponse
}

type appendCommand struct {
	ar communication.AppendRequest
	rc chan communication.AppendResponse
}

type proposalResult struct {
	index uint64
	err   error
}

type proposalCommand struct {
	entry common.Entry
	rc    chan proposalResult
}

var raftRand = makeRand()
var raftRandLock = &sync.Mutex{}

/*
StartRaft starts an instance of the raft implementation running.
It will start at least one goroutine for its implementation of the protocol,
and others to communicate with other nodes.
*/
func StartRaft(
	comm communication.Communication,
	stor storage.Storage,
	state StateMachine,
	configFile string) (*Service, error) {
	r := &Service{
		state:                int32(Follower),
		comm:                 comm,
		localAddress:         atomic.Value{},
		nodeConfig:           NodeList{},
		raftConfig:           config.GetDefaultConfig(),
		configFile:           configFile,
		stor:                 stor,
		stopChan:             make(chan chan bool, 1),
		loopCommands:         make(chan LoopCommand, 1),
		voteCommands:         make(chan voteCommand, 1),
		appendCommands:       make(chan appendCommand, 1),
		statusInquiries:      make(chan chan<- ProtocolStatus, 1),
		proposals:            make(chan proposalCommand, 100),
		latch:                sync.Mutex{},
		followerOnly:         false,
		appliedTracker:       CreateTracker(),
		stateMachine:         state,
		membershipChangeMode: int32(Stable),
	}

	var err error

	r.id, err = r.loadNodeID(LocalIDKey, stor, true)
	if err != nil {
		return nil, err
	}
	clusterID, err := r.loadNodeID(ClusterIDKey, stor, false)
	if err != nil {
		return nil, err
	}
	r.setClusterID(clusterID)

	glog.Infof("Node %s starting in cluster %s", r.id, clusterID)

	err = r.loadNodeConfig(stor)
	if err != nil {
		return nil, err
	}
	err = r.loadRaftConfig()
	if err != nil {
		return nil, err
	}

	r.lastIndex, r.lastTerm, err = r.stor.GetLastIndex()
	if err != nil {
		return nil, err
	}

	r.currentTerm = r.readCurrentTerm()
	r.commitIndex = r.readLastCommit()
	r.lastApplied = r.readLastApplied()

	go r.mainLoop()

	return r, nil
}

func (r *Service) loadNodeID(key string, stor storage.Storage, create bool) (common.NodeID, error) {
	id, err := stor.GetUintMetadata(key)
	if err != nil {
		return 0, err
	}
	if id == 0 && create {
		// Generate a random node ID
		id = uint64(randomInt64())
		err = stor.SetUintMetadata(key, id)
		if err != nil {
			return 0, err
		}
	}
	return common.NodeID(id), nil
}

func (r *Service) loadNodeConfig(stor storage.Storage) error {
	buf, err := stor.GetMetadata(NodeConfig)
	if err != nil {
		return err
	}

	if buf == nil {
		glog.Info("No cluster members detected: starting in standalone mode")
		r.setState(Standalone)
		return nil
	}

	cfg, err := decodeNodeList(buf)
	if err != nil {
		return err
	}
	r.setNodeConfig(cfg)
	return nil
}

func (r *Service) loadRaftConfig() error {
	if r.configFile == "" {
		return nil
	}

	_, err := os.Stat(r.configFile)
	if err == nil {
		// File exists and is readable
		return r.raftConfig.LoadFile(r.configFile)
	}

	// File probably does not exist
	glog.Infof("No configuration file found. Writing defaults to %s", r.configFile)
	return r.raftConfig.StoreFile(r.configFile)
}

/*
Close shuts the service down and stops its goroutines. It does not close
the database, however.
*/
func (r *Service) Close() {
	s := r.GetState()
	if s != Stopped && s != Stopping {
		done := make(chan bool)
		r.stopChan <- done
		<-done
	}
	r.appliedTracker.Close()
}

func (r *Service) cleanup() {
	for len(r.voteCommands) > 0 {
		glog.V(2).Info("Sending cleanup command for vote request")
		vc := <-r.voteCommands
		vc.rc <- communication.VoteResponse{
			Error: errors.New("Raft is shutting down"),
		}
	}
	//close(r.voteCommands)

	for len(r.appendCommands) > 0 {
		glog.V(2).Info("Sending cleanup command for append request")
		vc := <-r.appendCommands
		vc.rc <- communication.AppendResponse{
			Error: errors.New("Raft is shutting down"),
		}
	}
	//close(r.appendCommands)

	//close(r.receivedAppendChan)
}

/*
RequestVote is called from the communication interface when another node
requests a vote.
*/
func (r *Service) RequestVote(req communication.VoteRequest) (communication.VoteResponse, error) {
	if r.GetState() == Stopping || r.GetState() == Stopped {
		return communication.VoteResponse{}, errors.New("Raft is stopped")
	}

	rc := make(chan communication.VoteResponse)
	cmd := voteCommand{
		vr: req,
		rc: rc,
	}
	r.voteCommands <- cmd
	vr := <-rc
	return vr, vr.Error
}

/*
Append is called by the commnunication service when the leader has a new
item to append to the index.
*/
func (r *Service) Append(req communication.AppendRequest) (communication.AppendResponse, error) {
	glog.V(2).Infof("Node %s append request. State is %v", r.id, r.GetState())
	if r.GetState() == Stopping || r.GetState() == Stopped {
		return communication.AppendResponse{}, errors.New("Raft is stopped")
	}

	rc := make(chan communication.AppendResponse)
	cmd := appendCommand{
		ar: req,
		rc: rc,
	}

	r.appendCommands <- cmd
	resp := <-rc
	return resp, resp.Error
}

/*
Propose is called by anyone who wants to propose a new change. It will return
with the change number of the new change. However, that change number will
not necessarily have been committed yet.
*/
func (r *Service) Propose(e *common.Entry) (uint64, error) {
	if r.GetState() == Stopping || r.GetState() == Stopped {
		return 0, errors.New("Raft is stopped")
	}

	rc := make(chan proposalResult, 1)
	cmd := proposalCommand{
		entry: *e,
		rc:    rc,
	}

	glog.V(2).Infof("Going to propose a value of %d bytes and type %d",
		len(e.Data), e.Type)
	r.proposals <- cmd

	result := <-rc
	return result.index, result.err
}

/*
Join is called by the communication service when we are being added to a new
cluster and we need to catch up.
*/
func (r *Service) Join(req communication.JoinRequest) (uint64, error) {
	if r.GetClusterID() == 0 {
		lastIx, _, _ := r.stor.GetLastIndex()
		if lastIx > 0 {
			return 0, fmt.Errorf("Cannot join cluster because we already have data to index %d", lastIx)
		}
	} else if r.GetClusterID() != req.ClusterID {
		return 0, fmt.Errorf("Already part of cluster %s: Cannot join %s", r.GetClusterID(), req.ClusterID)
	}

	r.stor.SetUintMetadata(ClusterIDKey, uint64(req.ClusterID))
	r.setClusterID(req.ClusterID)

	for _, e := range req.Entries {
		r.stor.AppendEntry(&e)
	}
	for _, e := range req.ConfigEntries {
		r.handleJoinConfigEntry(&e)
	}

	if req.Last {
		r.loopCommands <- JoinAsFollower
	}

	lastIx, _, _ := r.stor.GetLastIndex()
	return lastIx, nil
}

/*
MyID returns the unique ID of this Raft node.
*/
func (r *Service) MyID() common.NodeID {
	return r.id
}

/*
GetState returns the state of this Raft node in a thread-safe way.
*/
func (r *Service) GetState() State {
	s := atomic.LoadInt32(&r.state)
	return State(s)
}

func (r *Service) setState(newState State) {
	glog.V(2).Infof("Node %s: setting state to %s", r.id, newState)
	ns := int32(newState)
	atomic.StoreInt32(&r.state, ns)
}

/*
GetLeaderID returns the unique ID of the leader node, or zero if there is
currently no known leader.
*/
func (r *Service) GetLeaderID() common.NodeID {
	return common.NodeID(atomic.LoadUint64(&r.leaderID))
}

func (r *Service) getLeader() *Node {
	return r.getNode(r.GetLeaderID())
}

func (r *Service) setLeader(id common.NodeID) {
	if id == 0 {
		glog.V(2).Infof("Node %s: No leader present", r.id)
	} else {
		glog.V(2).Infof("Node %s: Node %s is now the leader", r.id, id)
	}
	atomic.StoreUint64(&r.leaderID, uint64(id))
}

func (r *Service) getNode(id common.NodeID) *Node {
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.nodeConfig.GetNode(id)
}

/*
GetClusterID returns the unique identifier of the cluster where this instance
of the service runs. If the node is not in a cluster, then the cluster ID
will be zero.
*/
func (r *Service) GetClusterID() common.NodeID {
	return common.NodeID(atomic.LoadUint64(&r.clusterID))
}

func (r *Service) setClusterID(id common.NodeID) {
	atomic.StoreUint64(&r.clusterID, uint64(id))
}

/*
GetMembershipChangeMode gives us the status of the current process of
changing cluster membership.
*/
func (r *Service) GetMembershipChangeMode() MembershipChangeMode {
	return MembershipChangeMode(atomic.LoadInt32(&r.membershipChangeMode))
}

func (r *Service) setMembershipChangeMode(mode MembershipChangeMode) {
	atomic.StoreInt32(&r.membershipChangeMode, int32(mode))
}

/*
GetCurrentTerm returns the current Raft term.
*/
func (r *Service) GetCurrentTerm() uint64 {
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.currentTerm
}

func (r *Service) setCurrentTerm(t uint64) {
	// Use a mutex for this because we write it to DB and want that to be synchronized
	r.latch.Lock()
	defer r.latch.Unlock()
	r.currentTerm = t
	r.writeCurrentTerm(t)
}

/*
GetCommitIndex returns the current index that has been committed to a quorum
of nodes.
*/
func (r *Service) GetCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

// Atomically update the commit index, and return whether it changed
func (r *Service) setCommitIndex(t uint64) bool {
	oldIndex := atomic.SwapUint64(&r.commitIndex, t)
	return oldIndex != t
}

/*
GetLastApplied returns the current index that has been applied to this local
node.
*/
func (r *Service) GetLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

/*
This is where we finally apply the changes. Some changes are purely internal,
so we handle them here. Note that node configuration changes were handled
elsewhere.
*/
func (r *Service) setLastApplied(t uint64) {
	entry, err := r.stor.GetEntry(t)
	if err != nil {
		panic(fmt.Sprintf("Node %s: Error reading entry from change %d for commit: %s", r.id, t, err))
	}
	if entry == nil {
		// TODO maybe don't panic?
		panic(fmt.Sprintf("Node %s: Committed entry %d could not be read", r.id, t))
	}

	switch t := entry.Type; {

	case t == MembershipChange:
		r.applyMembershipChange(entry)

	case t == ConfigChange:
		r.applyRaftConfigChange(entry)
		r.loopCommands <- UpdateRaftConfiguration

	case t == PurgeRequest:
		r.purgeData(entry)

	case t >= 0:
		// Only pass positive (or zero) entry types to the state machine.
		err = r.stateMachine.Commit(entry)
		if err != nil {
			glog.Errorf("Error committing change %d: %s", t, err)
			return
		}
	}

	err = r.stor.SetUintMetadata(LastAppliedKey, t)
	if err != nil {
		panic(fmt.Sprintf("Node %s: Error updating last applied key %d to the database: %s", r.id, t, err))
	}

	atomic.StoreUint64(&r.lastApplied, t)

	r.appliedTracker.Update(t)
}

/*
handleJoinConfigEntry is invoked on special entries sent during the catch-up
process that help replicate config to a new node.
*/
func (r *Service) handleJoinConfigEntry(entry *common.Entry) {
	switch t := entry.Type; {

	case t == ConfigChange:
		r.applyRaftConfigChange(entry)

	default:
		// Ignore membership changes, as we will have that set up later.
		// Also ignore purge requests, since data we get should be pre-purged
		glog.V(2).Infof("Ignoring record of type %d during join", t)
	}
}

func (r *Service) applyRaftConfigChange(entry *common.Entry) {
	r.raftConfig.Load(entry.Data)
	if r.configFile != "" {
		err := r.raftConfig.StoreFile(r.configFile)
		if err != nil {
			glog.Errorf("Unable to persist new configuration file: %s", err)
		}
	}
}

/*
GetAppliedTracker returns a change tracker that can be used to wait until a
particular change number has been applied. This allows a caller who
recently proposed a new value to wait until the value has been applied
to a quorum of cluster nodes.
*/
func (r *Service) GetAppliedTracker() *ChangeTracker {
	return r.appliedTracker
}

/*
WaitForCommit blocks the caller until the specified index has been
applied across the quorum. It is useful for APIs that want to wait for
consistency before reporting to the user. It blocks for a maximum of
two election timeouts, which means that updates will always work as long
as the cluster is capable of electing a leader.
*/
func (r *Service) WaitForCommit(ix uint64) error {
	elTimeout := r.GetRaftConfig().ElectionTimeout()
	propTimeout := elTimeout * 2
	appliedIx := r.appliedTracker.TimedWait(ix, propTimeout)
	if appliedIx < ix {
		return errors.New("Proposal timeout -- change could not be committed to a quorum")
	}
	return nil
}

/*
GetLastIndex returns the highest index that exists in the local raft log,
and the corresponding term for that index.
*/
func (r *Service) GetLastIndex() (uint64, uint64) {
	// Use a mutex here so that both values are consistent
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.lastIndex, r.lastTerm
}

func (r *Service) setLastIndex(ix uint64, term uint64) {
	r.latch.Lock()
	defer r.latch.Unlock()
	r.lastIndex = ix
	r.lastTerm = term
}

/*
GetFirstIndex returns the lowest index that exists in the local raft log.
*/
func (r *Service) GetFirstIndex() (uint64, error) {
	return r.stor.GetFirstIndex()
}

/*
GetRaftStatus returns some status information about the Raft engine that requires
us to access internal state.
*/
func (r *Service) GetRaftStatus() ProtocolStatus {
	ch := make(chan ProtocolStatus)
	r.statusInquiries <- ch
	return <-ch
}

/*
UpdateConfiguration updates configuration of various aspects of the
implementation. The configuration will be pushed to the other nodes just like
any other change. It doesn't get actually applied until it gets proposed
to the various other nodes. This configuration will replace the configuration
files on every node in the cluster. The input is a set of YAML that matches
the YAML configuration syntax, as a byte slice.
*/
func (r *Service) UpdateConfiguration(cfg []byte) (uint64, error) {
	testCfg := config.GetDefaultConfig()
	err := testCfg.Load(cfg)
	if err != nil {
		return 0, err
	}

	return r.pushRaftConfiguration(cfg)
}

/*
UpdateLiveConfiguration makes the current configuration of this node
(as returned by GetRaftConfig) the live one across the cluster.
It will also update the local configuration file.
*/
func (r *Service) UpdateLiveConfiguration() (uint64, error) {
	cfg, err := r.raftConfig.Store()
	if err != nil {
		return 0, err
	}
	return r.pushRaftConfiguration(cfg)
}

func (r *Service) pushRaftConfiguration(cfg []byte) (uint64, error) {
	glog.V(2).Info("Updating the configuration across the cluster")
	entry := common.Entry{
		Type:      ConfigChange,
		Timestamp: time.Now(),
		Data:      cfg,
	}
	return r.Propose(&entry)
}

/*
GetNodeConfig returns the current configuration of this raft node, which means
the configuration that is currently running (as oppopsed to what
has been proposed.
*/
func (r *Service) GetNodeConfig() NodeList {
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.nodeConfig
}

func (r *Service) setNodeConfig(newCfg NodeList) error {
	encoded := newCfg.encode()
	err := r.stor.SetMetadata(NodeConfig, encoded)
	if err != nil {
		return err
	}

	r.latch.Lock()
	r.nodeConfig = newCfg
	r.latch.Unlock()
	return nil
}

/*
GetRaftConfig returns details about the state of the node, including cluster
status.
*/
func (r *Service) GetRaftConfig() *config.State {
	return r.raftConfig
}

func (r *Service) getLocalAddress() string {
	addr := r.localAddress.Load()
	if addr == nil {
		return ""
	}
	a := addr.(*string)
	return *a
}

/*
GetWebHooks returns the set of WebHook configuration that is currently configured
for this node.
*/
func (r *Service) GetWebHooks() []hooks.WebHook {
	return r.raftConfig.WebHooks()
}

// Used only in unit testing. Forces us to never become a leader.
func (r *Service) setFollowerOnly(f bool) {
	r.followerOnly = f
}

func (r *Service) readCurrentTerm() uint64 {
	ct, err := r.stor.GetUintMetadata(CurrentTermKey)
	if err != nil {
		panic("Fatal error reading state from database")
	}
	return ct
}

func (r *Service) writeCurrentTerm(ct uint64) {
	err := r.stor.SetUintMetadata(CurrentTermKey, ct)
	if err != nil {
		panic("Fatal error writing state to database")
	}
}

func (r *Service) readLastVote() common.NodeID {
	ct, err := r.stor.GetUintMetadata(VotedForKey)
	if err != nil {
		panic("Fatal error reading state from database")
	}
	return common.NodeID(ct)
}

func (r *Service) writeLastVote(ct common.NodeID) {
	err := r.stor.SetUintMetadata(VotedForKey, uint64(ct))
	if err != nil {
		panic("Fatal error writing state to database")
	}
}

func (r *Service) readLastCommit() uint64 {
	mi, _, err := r.stor.GetLastIndex()
	if err != nil {
		panic("Fatal error reading state from database")
	}
	return mi
}

func (r *Service) readLastApplied() uint64 {
	la, err := r.stor.GetUintMetadata(LastAppliedKey)
	if err != nil {
		panic("Fatal error reading state from state machine")
	}
	return la
}

// Election timeout is the default timeout, plus or minus one heartbeat interval.
// Use math.rand here, not crypto.rand, because it happens an awful lot.
func (r *Service) randomElectionTimeout() time.Duration {
	hbTimeout, elTimeout := r.GetRaftConfig().Timeouts()
	rge := int64(hbTimeout * 2)
	min := int64(elTimeout - hbTimeout)
	raftRandLock.Lock()
	defer raftRandLock.Unlock()
	return time.Duration(raftRand.Int63n(rge) + min)
}

/*
Use the crypto random number generator to initialize the regular one.
The regular one is just used to randomize election timeouts.
If we don't seed the generator, then a bunch of nodes won't get their
timeouts in a random way. But use a new generator here because the default
one might be used for various testing frameworks. Finally, be aware that
the non-default generator is not thread-safe!
*/
func makeRand() *rand.Rand {
	s := rand.NewSource(randomInt64())
	return rand.New(s)
}

var maxBigInt = big.NewInt(math.MaxInt64)

func randomInt64() int64 {
	ri, err := cryptoRand.Int(cryptoRand.Reader, maxBigInt)
	if err != nil {
		panic(err.Error())
	}
	return ri.Int64()
}
