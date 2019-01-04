package types

import (
	cb "github.com/hyperledger/fabric/protos/common"
	"unsafe"
)
type Message struct {
	ConfigSeq uint64
	NormalMsg *cb.Envelope
	ConfigMsg *cb.Envelope
}
type sliceMock struct {
	addr uintptr
	len  int
	cap  int
}

func (msg Message)ToByteSlice(){
	Len := unsafe.Sizeof(&msg)
	testBytes := &sliceMock{
		addr: uintptr(unsafe.Pointer(&msg)),
		cap:  int(Len),
		len:  int(Len),
	}
	return  *(*[]byte)(unsafe.Pointer(testBytes))

}
