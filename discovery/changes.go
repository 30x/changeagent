package discovery

func compareChanges(oldNodes []Node, newNodes []Node) []Change {
  var changes []Change

  // Know what IDs are in the old list
  existing := make(map[uint64]*Node)
  for i := range(oldNodes) {
    existing[oldNodes[i].Id] = &(oldNodes[i])
  }

  // Iterate through the new list for added and updated nodes
  for i := range(newNodes) {
    nn := &(newNodes[i])
    if existing[nn.Id] == nil {
      e := Change{
        Action: NewNode,
        Node: nn,
      }
      changes = append(changes, e)
    } else if existing[nn.Id].Address != nn.Address {
      e := Change{
        Action: UpdatedNode,
        Node: nn,
      }
      changes = append(changes, e)
    }
    delete(existing, nn.Id)
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
