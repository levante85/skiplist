package skiplist

import "fmt"

const nodesForBucket = 1024 * 1024

//Arena is an allocator type
type Arena struct {
	nodes     [][]Node
	available int
	current   int
}

func newArena() *Arena {
	arena := &Arena{
		nodes:     make([][]Node, 0),
		available: 0,
		current:   0,
	}

	nodes := make([]Node, nodesForBucket, nodesForBucket)
	arena.nodes = append(arena.nodes, nodes)
	arena.available = nodesForBucket

	return arena

}

//NodeFromID return the inderlying node pointer
func (a *Arena) NodeFromID(id NodeID) *Node {
	number := int(id) - 1
	if number < 0 {
		fmt.Println("num is: ", number)
	}
	bucket := number / nodesForBucket
	index := number % nodesForBucket

	return &a.nodes[bucket][index]
}

//ValueFromID return the inderlying node value
func (a *Arena) ValueFromID(id NodeID) []byte {
	number := int(id) - 1
	if number < 0 {
		return []byte{}
		// should checkout why getting id 0 should never happen
		// probably zero initialized elements in next
	}

	bucket := number / nodesForBucket
	index := number % nodesForBucket

	return a.nodes[bucket][index].Value
}

func (a *Arena) allocate(data []byte, height int) NodeID {
	a.available--
	newID := NodeID(a.current + 1)
	node := a.NodeFromID(newID)
	node.Value = data
	a.current++

	for i := 0; i <= height; i++ {
		node.Next = append(node.Next, NodeID(a.current+1))
		a.current++
		a.available--
	}

	return newID
}
