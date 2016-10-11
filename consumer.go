package cg

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/serialx/hashring"
)

const (
	consumerProtocolType       = "consumer"
	consumerGroupMemberVersion = 1
)

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
	Key() string
	Assign(Candidates) Assignment
}

// HashRing is a type of Protocol that attemps to handle additions/removals of
// nodes and partitions with the least number of changes to a previous configurations.
type HashRing struct {
}

// Key returns the string that Kafka makes sure all consumer group clients register as
// being able to handle.
func (hr *HashRing) Key() string {
	return "hashring"
}

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

// Config is used to create a new Consumer.
type Config struct {
	Client         sarama.Client
	GroupID        string
	Protocols      []Protocol
	SessionTimeout time.Duration
	Topics         []string
}

// Consumer implements a Kafka GroupConsumer with semantics available
// after Kafka 0.9.
type Consumer struct {
	client      sarama.Client
	cancel      func()
	cfg         Config
	ctx         context.Context
	err         error
	sessTimeout time.Duration
	m           chan *sarama.ConsumerMessage
	mid         string
	gid         int32
}

// NewConsumer creates a Kafka GroupConsumer.
func NewConsumer(cfg *Config) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		client: cfg.Client,
		cancel: cancel,
		cfg:    *cfg,
		ctx:    ctx,
		m:      make(chan *sarama.ConsumerMessage),
	}
	go c.run()
	return c
}

// Close returns when the Consumer has been shut down and output channel
// drained.
func (c *Consumer) Close() error {
	c.cancel()
	// TODO wrap in sync.Once
	// TODO do leaveGroup here
	return nil
}

func (c *Consumer) run() {
	for {
		sgr, err := c.join()
		if err != nil {
			c.err = err
			return
		}
		assignments, err := c.sync(sgr)
		if err != nil {
			c.err = err
			return
		}
		for topic, partitions := range assignments.Topics {
			for _, partition := range partitions {
				fmt.Printf("%s:%d\n", topic, partition)
				// TODO here is where you subscribe
			}
		}
		err = c.heartbeat()
	}
}

// groupAssignments is only called by the leader.
func (c *Consumer) groupAssignments(resp *sarama.JoinGroupResponse) (map[string]*sarama.ConsumerGroupMemberAssignment, error) {
	members, err := resp.GetMembers()
	if err != nil {
		return map[string]*sarama.ConsumerGroupMemberAssignment{}, err
	}
	cand := Candidates{
		MemberIDs:       make([]string, len(members)),
		TopicPartitions: make(map[string][]int32),
	}
	for memberID, meta := range members {
		cand.MemberIDs = append(cand.MemberIDs, memberID)
		for _, topic := range meta.Topics {
			partitions, err := c.client.Partitions(topic)
			if err != nil {
				return map[string]*sarama.ConsumerGroupMemberAssignment{}, err
			}
			cand.TopicPartitions[topic] = partitions
		}
	}
	for _, p := range c.cfg.Protocols {
		if p.Key() == resp.GroupProtocol {
			assignments := p.Assign(cand)
			result := make(map[string]*sarama.ConsumerGroupMemberAssignment, len(assignments))
			for memberID, tp := range assignments {
				result[memberID] = &sarama.ConsumerGroupMemberAssignment{
					Version: consumerGroupMemberVersion,
					Topics:  tp,
				}
			}
			return result, nil
		}
	}
	return map[string]*sarama.ConsumerGroupMemberAssignment{}, fmt.Errorf("unhandled protocol")
}

func (c *Consumer) heartbeat() error {
	t := time.NewTicker(c.cfg.SessionTimeout)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case <-t.C:
			b, err := c.client.Coordinator(c.cfg.GroupID)
			if err != nil {
				return err
			}
			resp, err := b.Heartbeat(&sarama.HeartbeatRequest{
				GroupId:      c.cfg.GroupID,
				MemberId:     c.mid,
				GenerationId: c.gid,
			})
			if err != nil {
				return err
			}
			switch resp.Err {
			case sarama.ErrNoError:
				continue // keep heartbeating.
			case sarama.ErrNotCoordinatorForConsumer, sarama.ErrRebalanceInProgress:
				return nil // exit heartbeat to rebalance.
			default:
				return resp.Err
			}
		}
	}
}

func (c *Consumer) join() (*sarama.SyncGroupRequest, error) {
	jgr, err := c.joinGroupRequest()
	if err != nil {
		return nil, err
	}
	b, err := c.client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return nil, err
	}
	resp, err := b.JoinGroup(jgr)
	if err != nil {
		return nil, err
	}
	// reset memberId if kafka has forgotten us.
	if resp.Err == sarama.ErrUnknownMemberId {
		c.mid = ""
	}
	if resp.Err != sarama.ErrNoError {
		return nil, resp.Err
	}
	c.mid = resp.MemberId
	c.gid = resp.GenerationId
	sgr := &sarama.SyncGroupRequest{
		GroupId:      c.cfg.GroupID,
		GenerationId: c.gid,
		MemberId:     c.mid,
	}
	if c.mid == resp.LeaderId {
		gas, err := c.groupAssignments(resp)
		for memberID, ga := range gas {
			sgr.AddGroupAssignmentMember(memberID, ga)
		}
		if err != nil {
			return nil, err
		}
	}
	return sgr, nil
}

func (c *Consumer) joinGroupRequest() (*sarama.JoinGroupRequest, error) {
	req := &sarama.JoinGroupRequest{
		GroupId:        c.cfg.GroupID,
		SessionTimeout: int32(c.cfg.SessionTimeout / time.Millisecond),
		// When a member first joins the group, the memberId will be empty (i.e. "").
		MemberId:     c.mid,
		ProtocolType: consumerProtocolType,
	}
	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: consumerGroupMemberVersion,
		Topics:  c.cfg.Topics,
	}
	for _, p := range c.cfg.Protocols {
		err := req.AddGroupProtocolMetadata(p.Key(), meta)
		if err != nil {
			return nil, err
		}
	}
	return req, nil
}

func (c *Consumer) sync(req *sarama.SyncGroupRequest) (*sarama.ConsumerGroupMemberAssignment, error) {
	b, err := c.client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return nil, err
	}
	resp, err := b.SyncGroup(req)
	if err != nil {
		return nil, err
	}
	return resp.GetMemberAssignment()
}
