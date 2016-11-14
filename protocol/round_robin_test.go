package protocol_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	cg "github.com/supershabam/sarama-cg"
	"github.com/supershabam/sarama-cg/protocol"
)

// Ensure RoundRobin implements Protocol interface
var _ cg.Protocol = &protocol.RoundRobin{}

func TestRoundRobin(t *testing.T) {
	rr := &protocol.RoundRobin{}
	cand := cg.Candidates{
		Members: []cg.Member{
			{
				MemberID: "m2",
			},
			{
				MemberID: "m1",
			},
			{
				MemberID: "m3",
			},
		},
		TopicPartitions: cg.TopicPartitions{
			"a": []int32{0, 1, 2, 3, 4, 5, 6, 7},
			"b": []int32{0},
		},
	}
	expect := cg.Assignment{
		"m1": cg.TopicPartitions{
			"a": []int32{0, 3, 6},
		},
		"m2": cg.TopicPartitions{
			"a": []int32{1, 4, 7},
		},
		"m3": cg.TopicPartitions{
			"a": []int32{2, 5},
			"b": []int32{0},
		},
	}
	assgn := rr.Assign(cand)
	if !reflect.DeepEqual(expect, assgn) {
		t.Error(spew.Sprintf("\nexpected:\n%#v\ngot:\n%#v\n", expect, assgn))
	}

}
