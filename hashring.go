package cg

import (
	"fmt"

	"github.com/serialx/hashring"
)

// HashRing is a type of Protocol that attemps to handle additions/removals of
// nodes and partitions with the least number of changes to a previous configurations.
type HashRing struct {
}

// Int32Slice is just so we can sort int32s.
type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }

// Assign distributes TopicPartitions among MemberIDs
func (hr *HashRing) Assign(cand Candidates) Assignment {
	a := make(map[string]topicPartitions, len(cand.MemberIDs))
	ring := hashring.New(cand.MemberIDs)
	for topic, partitions := range cand.TopicPartitions {
		for _, partition := range partitions {
			key := fmt.Sprintf("%s:%d", topic, partition)
			memberID, _ := ring.GetNode(key)
			if _, ok := a[memberID]; !ok {
				a[memberID] = map[string][]int32{}
			}
			if _, ok := a[memberID][topic]; !ok {
				a[memberID][topic] = []int32{}
			}
			a[memberID][topic] = append(a[memberID][topic], partition)
		}
	}
	return a
}
