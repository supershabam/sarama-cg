package cg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

const (
	consumerProtocolType       = "consumer"
	consumerGroupMemberVersion = 1
	offsetCommitRequestVersion = 2
	offsetFetchRequestVersion  = 1
)

// ProtocolKey is an implementation of a protocol that will be announced to
// the Kafka cluster under the given Key. All nodes in the group must agree
// on a common protocol.
type ProtocolKey struct {
	Protocol Protocol
	Key      string
}

// Consume is a function the Coordinator will call when it becomes responsible
// for a new topic-partition. The provided context will be canceled when the
// Coordinator is no longer responsible for the topic-partition. This function
// is expected not to block. If you encounter an error while reading the topic-
// partition, you are still seen as responsible for that topic-partition in
// the kafka consumer group, so you must either recover or stop the Coordinator
// to remove yourself from the consumer group.
type Consume func(ctx context.Context, topic string, partition int32)

// CoordinatorConfig is used to create a new Coordinator.
type CoordinatorConfig struct {
	Client         sarama.Client
	Context        context.Context
	GroupID        string
	Protocols      []ProtocolKey
	RetentionTime  time.Duration
	SessionTimeout time.Duration
	Heartbeat      time.Duration
	Topics         []string
}

// Coordinator implements a Kafka GroupConsumer with semantics available
// after Kafka 0.9.
type Coordinator struct {
	cancels map[string]map[int32]func()
	client  sarama.Client
	cfg     *CoordinatorConfig
	consume Consume
	ctx     context.Context
	gid     int32
	mid     string
	m       sync.RWMutex
}

// NewCoordinator creates a Kafka GroupConsumer.
func NewCoordinator(cfg *CoordinatorConfig) *Coordinator {
	c := &Coordinator{
		cancels: map[string]map[int32]func(){},
		client:  cfg.Client,
		cfg:     cfg,
		ctx:     cfg.Context,
	}
	return c
}

// Run executes the Coordinator until an error or the context provided at
// create time closes.
func (c *Coordinator) Run(consume Consume) error {
	c.consume = consume
	ctx, cancel := context.WithCancel(c.ctx)
	// ensure that ctx is canceled so that it propagates to all the
	// Consume functions we've called.
	defer cancel()
	// we run c.leaveGroup() notwithstanding the result of c.run and
	// return the first error between them both (if any)
	runErr := c.run(ctx)
	leaveErr := c.leaveGroup()
	if runErr != nil {
		return runErr
	}
	if leaveErr != nil {
		return leaveErr
	}
	return nil
}

// CommitOffset writes the provided offset for the topic-partition for the consumer group
// that this coordinator is participating as.
func (c *Coordinator) CommitOffset(topic string, partition int32, offset int64) error {
	b, err := c.client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return err
	}
	// OffsetCommitRequest retention time should be -1 to signify to use the broker default.
	var rt int64 = -1
	if c.cfg.RetentionTime.Nanoseconds() != 0 {
		rt = c.cfg.RetentionTime.Nanoseconds() / int64(time.Millisecond)
	}
	req := &sarama.OffsetCommitRequest{
		ConsumerGroup:           c.cfg.GroupID,
		ConsumerGroupGeneration: c.gid,
		ConsumerID:              c.mid,
		RetentionTime:           rt,
		Version:                 offsetCommitRequestVersion,
	}
	req.AddBlock(topic, partition, offset, 0, "")
	resp, err := b.CommitOffset(req)
	if err != nil {
		return err
	}
	// return first error we happen to iterate into (if any).
	for _, topicErrs := range resp.Errors {
		for _, partitionErr := range topicErrs {
			if partitionErr == sarama.ErrNoError {
				continue
			}
			return partitionErr
		}
	}
	return nil
}

// GetOffset returns the current committed offset for the topic-partition for the
// group id provided to the coordinator.
func (c *Coordinator) GetOffset(topic string, partition int32) (int64, error) {
	b, err := c.client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return 0, err
	}
	req := &sarama.OffsetFetchRequest{
		ConsumerGroup: c.cfg.GroupID,
		Version:       offsetFetchRequestVersion,
	}
	req.AddPartition(topic, partition)
	resp, err := b.FetchOffset(req)
	if err != nil {
		return 0, err
	}
	block := resp.GetBlock(topic, partition)
	if block == nil {
		return 0, fmt.Errorf("nil block returned from fetch offset")
	}
	if block.Err != sarama.ErrNoError {
		return 0, block.Err
	}
	return block.Offset, nil
}

// groupAssignments is only called by the leader and is responsible for assigning
// topic-partitions to the members in the group via a protocol common to the group.
func (c *Coordinator) groupAssignments(resp *sarama.JoinGroupResponse) (map[string]*sarama.ConsumerGroupMemberAssignment, error) {
	// build Candidates.
	members, err := resp.GetMembers()
	if err != nil {
		return map[string]*sarama.ConsumerGroupMemberAssignment{}, err
	}
	cand := Candidates{
		MemberIDs:       make([]string, 0, len(members)),
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
	// determine which protocol to use to assign topic-partitions.
	for _, p := range c.cfg.Protocols {
		if p.Key == resp.GroupProtocol {
			assignments := p.Protocol.Assign(cand)
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

func (c *Coordinator) heartbeat(ctx context.Context) error {
	t := time.NewTicker(c.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
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

func (c *Coordinator) leaveGroup() error {
	// if we were never part of a group.
	if c.mid == "" {
		return nil
	}
	b, err := c.client.Coordinator(c.cfg.GroupID)
	if err != nil {
		return err
	}
	resp, err := b.LeaveGroup(&sarama.LeaveGroupRequest{
		GroupId:  c.cfg.GroupID,
		MemberId: c.mid,
	})
	if err != nil {
		return err
	}
	if resp.Err != sarama.ErrNoError {
		return resp.Err
	}
	return nil
}

func (c *Coordinator) join() (*sarama.SyncGroupRequest, error) {
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

func (c *Coordinator) joinGroupRequest() (*sarama.JoinGroupRequest, error) {
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
		err := req.AddGroupProtocolMetadata(p.Key, meta)
		if err != nil {
			return nil, err
		}
	}
	return req, nil
}

func (c *Coordinator) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		sgr, err := c.join()
		if err != nil {
			return err
		}
		myAssignments, err := c.sync(sgr)
		if err != nil {
			return err
		}
		err = c.set(ctx, myAssignments)
		if err != nil {
			return err
		}
		err = c.heartbeat(ctx)
		if err != nil {
			return err
		}
	}
}

func (c *Coordinator) set(ctx context.Context, assignments *sarama.ConsumerGroupMemberAssignment) error {
	for topic, partitions := range assignments.Topics {
		// ensure topic entry is created.
		if _, ok := c.cancels[topic]; !ok {
			c.cancels[topic] = map[int32]func(){}
		}
		// remove consumers we already created, but are no longer responsible for.
	MyNextPartition:
		for myPartition, cancel := range c.cancels[topic] {
			for _, targetPartition := range partitions {
				if myPartition == targetPartition {
					continue MyNextPartition
				}
			}
			cancel()
			delete(c.cancels[topic], myPartition)
		}
		for _, partition := range partitions {
			// continue if we're already handling the topic-partition.
			if _, ok := c.cancels[topic][partition]; ok {
				continue
			}
			// start handling new topic-partition.
			ctx, cancel := context.WithCancel(ctx)
			c.cancels[topic][partition] = cancel
			// we call the provided Consume function in a goroutine so that the implementor of this function
			// can't accidentally block the coordinator.
			go c.consume(ctx, topic, partition)
		}
	}
	return nil
}

func (c *Coordinator) sync(req *sarama.SyncGroupRequest) (*sarama.ConsumerGroupMemberAssignment, error) {
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
