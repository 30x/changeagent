package discovery

func getChangeType(old, new []Node) int {
  if nodeIdsEqual(old, new) {
    for k, v := range(old) {
      if v.Address != new[k].Address {
        return AddressesChanged
      }
    }
    return 0
  }
  return NodesChanged
}

func nodeIdsEqual(old, new []Node) bool {
  if len(old) == len(new) {
    for k, v := range (old) {
      if v.ID != new[k].ID {
        return false
      }
    }
    return true
  }
  return false
}

/* Old code to compare change lists

func compareChanges(oldNodes []Node, newNodes []Node) []Change {
  var changes []Change

  // Know what IDs are in the old list
  existing := make(map[uint64]*Node)
  for i := range(oldNodes) {
    existing[oldNodes[i].ID] = &(oldNodes[i])
  }

  // Iterate through the new list for added and updated nodes
  for i := range(newNodes) {
    nn := &(newNodes[i])
    if existing[nn.ID] == nil {
      e := Change{
        Action: NewNode,
        Node: nn,
      }
      changes = append(changes, e)
    } else if existing[nn.ID].Address != nn.Address {
      e := Change{
        Action: UpdatedNode,
        Node: nn,
      }
      changes = append(changes, e)
    }
    delete(existing, nn.ID)
  }

  // Anything left is left over and must be deleted
  for _, dn := range(existing) {
    e := Change{
      Action: DeletedNode,
      Node: dn,
    }
    changes = append(changes, e)
  }

  return changes
}

*/
