package protocol

import (
	"github.com/supershabam/sarama-cg"
)

var _ cg.Protocol = &HashRing{}
