// Code generated by protoc-gen-go.
// source: common.proto
// DO NOT EDIT!

/*
Package protobufs is a generated protocol buffer package.

It is generated from these files:
	common.proto
	communication.proto
	nodelist.proto

It has these top-level messages:
	EntryPb
	VoteRequestPb
	VoteResponsePb
	AppendRequestPb
	JoinRequestPb
	AppendResponsePb
	ProposalResponsePb
	DiscoveryResponsePb
	NodePb
	NodeListPb
*/
package protobufs

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

// A single Raft entry.
type EntryPb struct {
	Index            *uint64  `protobuf:"varint,1,req,name=index" json:"index,omitempty"`
	Type             *int32   `protobuf:"varint,2,req,name=type" json:"type,omitempty"`
	Term             *uint64  `protobuf:"varint,3,opt,name=term" json:"term,omitempty"`
	Timestamp        *int64   `protobuf:"varint,4,opt,name=timestamp" json:"timestamp,omitempty"`
	Tags             []string `protobuf:"bytes,5,rep,name=tags" json:"tags,omitempty"`
	Data             []byte   `protobuf:"bytes,6,opt,name=data" json:"data,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *EntryPb) Reset()                    { *m = EntryPb{} }
func (m *EntryPb) String() string            { return proto.CompactTextString(m) }
func (*EntryPb) ProtoMessage()               {}
func (*EntryPb) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *EntryPb) GetIndex() uint64 {
	if m != nil && m.Index != nil {
		return *m.Index
	}
	return 0
}

func (m *EntryPb) GetType() int32 {
	if m != nil && m.Type != nil {
		return *m.Type
	}
	return 0
}

func (m *EntryPb) GetTerm() uint64 {
	if m != nil && m.Term != nil {
		return *m.Term
	}
	return 0
}

func (m *EntryPb) GetTimestamp() int64 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

func (m *EntryPb) GetTags() []string {
	if m != nil {
		return m.Tags
	}
	return nil
}

func (m *EntryPb) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterType((*EntryPb)(nil), "protobufs.EntryPb")
}

func init() { proto.RegisterFile("common.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 134 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0xe2, 0xe2, 0x49, 0xce, 0xcf, 0xcd,
	0xcd, 0xcf, 0xd3, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x04, 0x53, 0x49, 0xa5, 0x69, 0xc5,
	0x4a, 0xc9, 0x5c, 0xec, 0xae, 0x79, 0x25, 0x45, 0x95, 0x01, 0x49, 0x42, 0xbc, 0x5c, 0xac, 0x99,
	0x79, 0x29, 0xa9, 0x15, 0x12, 0x8c, 0x0a, 0x4c, 0x1a, 0x2c, 0x42, 0x3c, 0x5c, 0x2c, 0x25, 0x95,
	0x05, 0xa9, 0x12, 0x4c, 0x40, 0x1e, 0x2b, 0x98, 0x97, 0x5a, 0x94, 0x2b, 0xc1, 0xac, 0xc0, 0x08,
	0x94, 0x13, 0xe4, 0xe2, 0x2c, 0xc9, 0xcc, 0x4d, 0x2d, 0x2e, 0x49, 0xcc, 0x2d, 0x90, 0x60, 0x01,
	0x0a, 0x31, 0x83, 0x15, 0x24, 0xa6, 0x17, 0x4b, 0xb0, 0x2a, 0x30, 0x6b, 0x70, 0x82, 0x78, 0x29,
	0x89, 0x25, 0x89, 0x12, 0x6c, 0x40, 0x39, 0x1e, 0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0xd6, 0x1b,
	0xb3, 0xcf, 0x7e, 0x00, 0x00, 0x00,
}
