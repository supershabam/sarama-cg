package protocol

import (
	"sort"

	cg "github.com/supershabam/sarama-cg"
)

// RoundRobin assigns partitions to members by assigning one partition to each
// member in-order until all partitions are assigned per topic. MemberIDs should be sorted,
// and partitions should be sorted ascending before assignment.
//
// e.g. members: [m1, m2, m3], topicpartitions: {a: [0, 1, 2, 3, 4, 5, 6, 7], b:[0]}
// assignemnts: {m1: {a: [0, 3, 6], b: []}, m2: {a: [1, 4, 7], b: []}, m3: {a: [2, 5], b:[0]}}
type RoundRobin struct {
	MyUserData []byte
}

// Assign implements the round-robin assignment.
func (rr *RoundRobin) Assign(cand cg.Candidates) cg.Assignment {
	assgn := make(map[string]cg.TopicPartitions, len(cand.Members))
	// sorted member ids
	smids := make([]string, 0, len(cand.Members))
	for _, m := range cand.Members {
		smids = append(smids, m.MemberID)
	}
	sort.Strings(smids)
	// sorted topics
	st := make([]string, 0, len(cand.TopicPartitions))
	for topic := range cand.TopicPartitions {
		st = append(st, topic)
	}
	sort.Strings(st)
	// member index to increment as we make assigments
	var midx int
	for _, topic := range st {
		partitions := int32Slice(cand.TopicPartitions[topic])
		sort.Sort(partitions)
		for _, partition := range partitions {
			mid := smids[midx]
			midx = (midx + 1) % len(smids)
			if _, ok := assgn[mid]; !ok {
				assgn[mid] = cg.TopicPartitions{}
			}
			tp := assgn[mid]
			if _, ok := tp[topic]; !ok {
				tp[topic] = []int32{}
			}
			tp[topic] = append(tp[topic], partition)
		}
	}
	return assgn
}

// UserData is written to Kafka upon joining the group and can be accessed within
// the Assign funcion utilizing the UserData from all other members.
func (rr *RoundRobin) UserData() []byte {
	return rr.MyUserData
}
