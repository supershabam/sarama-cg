package cg

type topicPartitions map[string][]int32

// Candidates represents all members of the Consumer Group which need to decide
// how to devide up all the TopicPartitions among themselves.
type Candidates struct {
	MemberIDs       []string
	TopicPartitions topicPartitions
}

// Assignment is the result of a Protocol determining which TopicPartitions from
// Candidates should be assigned to specific MembersIDs.
type Assignment map[string]topicPartitions

// Protocol is an agreed-upon method between all consumer group members of how to
// divide TopicPartitions amongst themselves.
type Protocol interface {
	Assign(Candidates) Assignment
}
