package types

import (
	cb "github.com/hyperledger/fabric/protos/common"
)
type Message struct {
	ConfigSeq uint64
	NormalMsg *cb.Envelope
	ConfigMsg *cb.Envelope
}
