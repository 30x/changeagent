package discovery

const (
  StateNew = iota
  StateCatchup = iota
  StateMember = iota
  StateDeleting = iota

  NewNode = iota
  DeletedNode = iota
  UpdatedNode = iota
)

type Node struct {
  Id uint64
  Address string
  State int
}

type Change struct {
  Action int
  Node Node
}

type Discovery interface {
  GetNodes() []Node
  GetAddress(id uint64) string
  AddNode(node *Node) error
  RemoveNode(id uint64) error
  //UpdateAddress(id uint64, address string) error
  //UpdateState(id uint64, state int) error
  GetChanges() <-chan Change
  Close()
}
