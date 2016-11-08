package cg

// TopicPartitions is a map of topics and the partition numbers in that topic.
type TopicPartitions map[string][]int32

// Member represents a consumer in a consumer group. The MemberID is assigned by Kafka
// and the UserData is provided by the consumer when it joins the group via the Protocol
// that was common between all consumers.
type Member struct {
	MemberID string
	UserData []byte
}

// Candidates represents all members of the Consumer Group which need to decide
// how to devide up all the TopicPartitions among themselves.
type Candidates struct {
	Members         []Member
	TopicPartitions TopicPartitions
}

// Assignment is the result of a Protocol determining which TopicPartitions from
// Candidates should be assigned to specific MembersIDs.
type Assignment map[string]TopicPartitions

// Protocol is an agreed-upon method between all consumer group members of how to
// divide TopicPartitions amongst themselves.
type Protocol interface {
	Assign(Candidates) Assignment
	UserData() []byte
}
