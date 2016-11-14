package protocol

import (
	"fmt"

	"github.com/supershabam/sarama-cg"

	"github.com/serialx/hashring"
)

// HashRing is a type of Protocol that attemps to handle additions/removals of
// nodes and partitions with the least number of changes to a previous configurations.
type HashRing struct {
	MyUserData []byte
}

// Int32Slice is just so we can sort int32s.
type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }

// Assign distributes TopicPartitions among MemberIDs
func (hr *HashRing) Assign(cand cg.Candidates) cg.Assignment {
	a := make(map[string]cg.TopicPartitions, len(cand.Members))
	ids := make([]string, 0, len(cand.Members))
	for _, member := range cand.Members {
		ids = append(ids, member.MemberID)
	}
	ring := hashring.New(ids)
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

// UserData is written to Kafka upon joining the group and can be accessed within
// the Assign funcion utilizing the UserData from all other members.
func (hr *HashRing) UserData() []byte {
	return hr.MyUserData
}
