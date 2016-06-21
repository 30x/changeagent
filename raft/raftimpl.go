package raft

import (
	cryptoRand "crypto/rand"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/30x/changeagent/communication"
	"github.com/30x/changeagent/discovery"
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
	LastAppliedKey = "lastApplied"
	NodeConfig     = "nodeConfig"
	WebHooks       = "webHooks"
)

const (
	// MembershipChange denotes a special message type for membership changes.
	MembershipChange = -1
	// WebHookChange denotes a change in the WebHook configuration for the
	// cluster.
	WebHookChange = -2

	// ElectionTimeout is the amount of time a node will wait once it has heard
	// from the current leader before it declares itself a candidate.
	// It must always be a small multiple of HeartbeatTimeout.
	ElectionTimeout = 10 * time.Second
	// HeartbeatTimeout is the amount of time between heartbeat messages from the
	// leader to other nodes.
	HeartbeatTimeout = 2 * time.Second

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

/*
Service is an instance of code that implements the Raft protocol.
It relies on the Storage, Discovery, and Communication services to do
its work, and invokes the StateMachine when changes are committed.
*/
type Service struct {
	id                  communication.NodeID
	localAddress        atomic.Value
	state               int32
	leaderID            uint64
	comm                communication.Communication
	disco               discovery.Discovery
	nodeDisco           *nodeDiscovery
	nodeConfig          atomic.Value
	configChanges       <-chan bool
	stor                storage.Storage
	stopChan            chan chan bool
	voteCommands        chan voteCommand
	appendCommands      chan appendCommand
	proposals           chan proposalCommand
	statusInquiries     chan chan<- ProtocolStatus
	discoveredNodes     map[communication.NodeID]string
	discoveredAddresses map[string]communication.NodeID
	latch               sync.Mutex
	followerOnly        bool
	currentTerm         uint64
	commitIndex         uint64
	lastApplied         uint64
	lastIndex           uint64
	lastTerm            uint64
	appliedTracker      *ChangeTracker
	stateMachine        StateMachine
	webHooks            atomic.Value
}

/*
ProtocolStatus returns some of the diagnostic information from the raft engine.
*/
type ProtocolStatus struct {
	// Status of the membership change process
	ChangeMode MembershipChangeMode
	// If this node is the leader, a map of the indices of each peer.
	// Otherwise nil.
	PeerIndices *map[string]uint64
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
	entry storage.Entry
	rc    chan proposalResult
}

var raftRand = makeRand()
var raftRandLock = &sync.Mutex{}

/*
StartRaft starts an instance of the raft implementation running.
It will start at least one goroutine for its implementation of the protocol,
and others to communicate with other nodes.
*/
func StartRaft(comm communication.Communication,
	disco discovery.Discovery,
	stor storage.Storage,
	state StateMachine) (*Service, error) {
	r := &Service{
		state:               int32(Follower),
		comm:                comm,
		localAddress:        atomic.Value{},
		nodeConfig:          atomic.Value{},
		stor:                stor,
		disco:               disco,
		stopChan:            make(chan chan bool, 1),
		voteCommands:        make(chan voteCommand, 1),
		appendCommands:      make(chan appendCommand, 1),
		statusInquiries:     make(chan chan<- ProtocolStatus, 1),
		proposals:           make(chan proposalCommand, 100),
		discoveredNodes:     make(map[communication.NodeID]string),
		discoveredAddresses: make(map[string]communication.NodeID),
		latch:               sync.Mutex{},
		followerOnly:        false,
		appliedTracker:      CreateTracker(),
		stateMachine:        state,
		webHooks:            atomic.Value{},
	}

	nodeID, err := stor.GetUintMetadata(LocalIDKey)
	if err != nil {
		return nil, err
	}
	if nodeID == 0 {
		// Generate a random node ID
		nodeID = uint64(randomInt64())
		err = stor.SetUintMetadata(LocalIDKey, nodeID)
		if err != nil {
			return nil, err
		}
	}
	r.id = communication.NodeID(nodeID)
	glog.Infof("Node %s starting", r.id)

	err = r.loadCurrentConfig(disco, stor)
	if err != nil {
		return nil, err
	}
	err = r.loadWebHooks(stor)
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

	if len(disco.GetCurrentConfig().Current.New) == 1 {
		glog.Info("Only one node. Starting in leader mode.\n")
		r.state = int32(Leader)
	}

	r.configChanges = disco.Watch()

	r.nodeDisco = startNodeDiscovery(disco, comm, r)

	go r.mainLoop()

	return r, nil
}

func (r *Service) loadCurrentConfig(disco discovery.Discovery, stor storage.Storage) error {
	buf, err := stor.GetMetadata(NodeConfig)
	if err != nil {
		return err
	}

	if buf == nil || disco.IsStandalone() {
		glog.Info("Loading node configuration for the first time")
		cfg := disco.GetCurrentConfig()
		var storBuf []byte
		storBuf, err = discovery.EncodeConfig(cfg)
		if err != nil {
			return err
		}
		err = stor.SetMetadata(NodeConfig, storBuf)
		if err != nil {
			return err
		}
		r.nodeConfig.Store(cfg)
		return nil
	}

	cfg, err := discovery.DecodeConfig(buf)
	if err != nil {
		return err
	}
	r.nodeConfig.Store(cfg)
	return nil
}

func (r *Service) loadWebHooks(stor storage.Storage) error {
	buf, err := stor.GetMetadata(WebHooks)
	if err != nil {
		return err
	}

	var webHooks []hooks.WebHook
	if buf != nil {
		webHooks, err = hooks.DecodeHooks(buf)
		if err != nil {
			return err
		}
	}

	r.webHooks.Store(webHooks)
	return nil
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
	r.nodeDisco.stop()
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
	glog.V(2).Infof("Node %d append request. State is %v", r.id, r.GetState())
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
func (r *Service) Propose(e storage.Entry) (uint64, error) {
	if r.GetState() == Stopping || r.GetState() == Stopped {
		return 0, errors.New("Raft is stopped")
	}

	rc := make(chan proposalResult, 1)
	cmd := proposalCommand{
		entry: e,
		rc:    rc,
	}

	glog.V(2).Infof("Going to propose a value of %d bytes and type %d",
		len(e.Data), e.Type)
	r.proposals <- cmd

	result := <-rc
	return result.index, result.err
}

/*
MyID returns the unique ID of this Raft node.
*/
func (r *Service) MyID() communication.NodeID {
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
	glog.V(2).Infof("Node %s: setting state to %d", r.id, newState)
	ns := int32(newState)
	atomic.StoreInt32(&r.state, ns)
}

/*
GetLeaderID returns the unique ID of the leader node, or zero if there is
currently no known leader.
*/
func (r *Service) GetLeaderID() communication.NodeID {
	return communication.NodeID(atomic.LoadUint64(&r.leaderID))
}

func (r *Service) setLeaderID(newID communication.NodeID) {
	if newID == 0 {
		glog.V(2).Infof("Node %s: No leader present", r.id)
	} else {
		glog.V(2).Infof("Node %s: Node %d is now the leader", r.id, newID)
	}
	atomic.StoreUint64(&r.leaderID, uint64(newID))
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
		glog.Errorf("Error reading entry from change %d for commit: %s", t, err)
		return
	}
	if entry == nil {
		glog.Errorf("Committed entry %d could not be read", t)
		return
	}

	switch t := entry.Type; {
	case t == WebHookChange:
		r.applyWebHookChange(entry)

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
		glog.Errorf("Error updating last applied key %d to the database: %s", t, err)
		return
	}

	atomic.StoreUint64(&r.lastApplied, t)

	r.appliedTracker.Update(t)
}

func (r *Service) applyWebHookChange(entry *storage.Entry) {
	hooks, err := hooks.DecodeHooksJSON(entry.Data)
	if err != nil {
		glog.Errorf("Error receiving web hook change data")
		return
	}

	glog.Info("Updating the web hook configuration on the server")
	r.setWebHooks(hooks)
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
UpdateWebHooks updates the configuration of web hooks for the cluster by
propagating a special change record to all the nodes. A web hook is a
particular web service URI that the leader will invoke before trying to commit any
new change -- if any one of the hooks fails, the leader will not make the change.
*/
func (r *Service) UpdateWebHooks(webHooks []hooks.WebHook) (uint64, error) {
	glog.V(2).Infof("Starting update to %d web hooks", len(webHooks))
	json := hooks.EncodeHooksJSON(webHooks)
	entry := storage.Entry{
		Type:      WebHookChange,
		Timestamp: time.Now(),
		Data:      json,
	}
	return r.Propose(entry)
}

/*
GetNodeConfig returns the current configuration of this raft node, which means
the configuration that is currently running (as oppopsed to what
has been proposed.
*/
func (r *Service) GetNodeConfig() *discovery.NodeConfig {
	return r.nodeConfig.Load().(*discovery.NodeConfig)
}

func (r *Service) setNodeConfig(newCfg *discovery.NodeConfig) error {
	encoded, err := discovery.EncodeConfig(newCfg)
	if err != nil {
		return err
	}
	err = r.stor.SetMetadata(NodeConfig, encoded)
	if err != nil {
		return err
	}
	r.nodeConfig.Store(newCfg)
	return nil
}

func (r *Service) addDiscoveredNode(id communication.NodeID, addr string) {
	r.latch.Lock()
	r.discoveredNodes[id] = addr
	r.discoveredAddresses[addr] = id
	r.latch.Unlock()
	if id == r.id {
		r.localAddress.Store(&addr)
	}
}

func (r *Service) getNodeAddress(id communication.NodeID) string {
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.discoveredNodes[id]
}

func (r *Service) getNodeID(address string) communication.NodeID {
	r.latch.Lock()
	defer r.latch.Unlock()
	return r.discoveredAddresses[address]
}

func (r *Service) getLocalAddress() string {
	addr := r.localAddress.Load().(*string)
	if addr == nil {
		return ""
	}
	return *addr
}

/*
GetWebHooks returns the set of WebHook configuration that is currently configured
for this node.
*/
func (r *Service) GetWebHooks() []hooks.WebHook {
	return r.webHooks.Load().([]hooks.WebHook)
}

func (r *Service) setWebHooks(h []hooks.WebHook) {
	buf := hooks.EncodeHooks(h)
	r.stor.SetMetadata(WebHooks, buf)
	r.webHooks.Store(h)
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

func (r *Service) readLastVote() communication.NodeID {
	ct, err := r.stor.GetUintMetadata(VotedForKey)
	if err != nil {
		panic("Fatal error reading state from database")
	}
	return communication.NodeID(ct)
}

func (r *Service) writeLastVote(ct communication.NodeID) {
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
	rge := int64(HeartbeatTimeout * 2)
	min := int64(ElectionTimeout - HeartbeatTimeout)
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
