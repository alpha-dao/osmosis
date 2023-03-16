// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: osmosis/concentrated-liquidity/pool.proto

// This is a legacy package that requires additional migration logic
// in order to use the correct package. Decision made to use legacy package path
// until clear steps for migration logic and the unknowns for state breaking are
// investigated for changing proto package.

package model

import (
	fmt "fmt"
	_ "github.com/cosmos/cosmos-proto"
	github_com_cosmos_cosmos_sdk_types "github.com/cosmos/cosmos-sdk/types"
	_ "github.com/gogo/protobuf/gogoproto"
	proto "github.com/gogo/protobuf/proto"
	_ "github.com/gogo/protobuf/types"
	github_com_gogo_protobuf_types "github.com/gogo/protobuf/types"
	io "io"
	math "math"
	math_bits "math/bits"
	time "time"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf
var _ = time.Kitchen

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Pool struct {
	Address string `protobuf:"bytes,1,opt,name=address,proto3" json:"address,omitempty" yaml:"address"`
	Id      uint64 `protobuf:"varint,2,opt,name=id,proto3" json:"id,omitempty"`
	// Amount of total liquidity
	CurrentTickLiquidity github_com_cosmos_cosmos_sdk_types.Dec `protobuf:"bytes,3,opt,name=current_tick_liquidity,json=currentTickLiquidity,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Dec" json:"current_tick_liquidity" yaml:"current_tick_liquidity"`
	Token0               string                                 `protobuf:"bytes,4,opt,name=token0,proto3" json:"token0,omitempty"`
	Token1               string                                 `protobuf:"bytes,5,opt,name=token1,proto3" json:"token1,omitempty"`
	CurrentSqrtPrice     github_com_cosmos_cosmos_sdk_types.Dec `protobuf:"bytes,6,opt,name=current_sqrt_price,json=currentSqrtPrice,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Dec" json:"current_sqrt_price" yaml:"spot_price"`
	CurrentTick          github_com_cosmos_cosmos_sdk_types.Int `protobuf:"bytes,7,opt,name=current_tick,json=currentTick,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Int" json:"current_tick" yaml:"current_tick"`
	// tick_spacing must be one of the authorized_tick_spacing values set in the
	// concentrated-liquidity parameters
	TickSpacing               uint64                                 `protobuf:"varint,8,opt,name=tick_spacing,json=tickSpacing,proto3" json:"tick_spacing,omitempty" yaml:"tick_spacing"`
	PrecisionFactorAtPriceOne github_com_cosmos_cosmos_sdk_types.Int `protobuf:"bytes,9,opt,name=precision_factor_at_price_one,json=precisionFactorAtPriceOne,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Int" json:"precision_factor_at_price_one" yaml:"precision_factor_at_price_one"`
	// swap_fee is the ratio that is charged on the amount of token in.
	SwapFee github_com_cosmos_cosmos_sdk_types.Dec `protobuf:"bytes,10,opt,name=swap_fee,json=swapFee,proto3,customtype=github.com/cosmos/cosmos-sdk/types.Dec" json:"swap_fee" yaml:"swap_fee"`
	// last_liquidity_update is the last time either the pool liquidity or the
	// active tick changed
	LastLiquidityUpdate time.Time `protobuf:"bytes,11,opt,name=last_liquidity_update,json=lastLiquidityUpdate,proto3,stdtime" json:"last_liquidity_update" yaml:"last_liquidity_update"`
}

func (m *Pool) Reset()      { *m = Pool{} }
func (*Pool) ProtoMessage() {}
func (*Pool) Descriptor() ([]byte, []int) {
	return fileDescriptor_3526ea5373d96c9a, []int{0}
}
func (m *Pool) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Pool) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Pool.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Pool) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Pool.Merge(m, src)
}
func (m *Pool) XXX_Size() int {
	return m.Size()
}
func (m *Pool) XXX_DiscardUnknown() {
	xxx_messageInfo_Pool.DiscardUnknown(m)
}

var xxx_messageInfo_Pool proto.InternalMessageInfo

func init() {
	proto.RegisterType((*Pool)(nil), "osmosis.concentratedliquidity.v1beta1.Pool")
}

func init() {
	proto.RegisterFile("osmosis/concentrated-liquidity/pool.proto", fileDescriptor_3526ea5373d96c9a)
}

var fileDescriptor_3526ea5373d96c9a = []byte{
	// 610 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x54, 0x41, 0x4f, 0xd4, 0x40,
	0x14, 0xde, 0x22, 0xb0, 0x30, 0x4b, 0x50, 0x06, 0xc4, 0x42, 0x64, 0x4b, 0x1a, 0x35, 0x6b, 0xe2,
	0xb6, 0xae, 0xc6, 0x0b, 0x37, 0x56, 0x25, 0x21, 0x31, 0x81, 0x14, 0xf4, 0x60, 0x48, 0x9a, 0xd9,
	0xe9, 0x50, 0x26, 0xdb, 0x76, 0xba, 0x9d, 0x59, 0x84, 0x1f, 0x60, 0xe2, 0x91, 0x23, 0x89, 0x17,
	0x7e, 0x84, 0x3f, 0x82, 0x78, 0xe2, 0x68, 0x3c, 0x54, 0xc3, 0xfe, 0x83, 0xfd, 0x05, 0xa6, 0xd3,
	0xe9, 0xd2, 0x44, 0x34, 0xd9, 0x53, 0xfb, 0xde, 0xf7, 0xe6, 0x7b, 0xdf, 0x7b, 0x6f, 0xde, 0x80,
	0xa7, 0x8c, 0x87, 0x8c, 0x53, 0x6e, 0x63, 0x16, 0x61, 0x12, 0x89, 0x04, 0x09, 0xe2, 0x35, 0x03,
	0xda, 0xeb, 0x53, 0x8f, 0x8a, 0x53, 0x3b, 0x66, 0x2c, 0xb0, 0xe2, 0x84, 0x09, 0x06, 0x1f, 0xab,
	0x50, 0xab, 0x1c, 0x3a, 0x8a, 0xb4, 0x8e, 0x5b, 0x1d, 0x22, 0x50, 0x6b, 0x75, 0x05, 0xcb, 0x38,
	0x57, 0x1e, 0xb2, 0x73, 0x23, 0x67, 0x58, 0x5d, 0xf2, 0x99, 0xcf, 0x72, 0x7f, 0xf6, 0xa7, 0xbc,
	0x86, 0xcf, 0x98, 0x1f, 0x10, 0x5b, 0x5a, 0x9d, 0xfe, 0xa1, 0x2d, 0x68, 0x48, 0xb8, 0x40, 0x61,
	0x9c, 0x07, 0x98, 0x5f, 0xab, 0x60, 0x72, 0x97, 0xb1, 0x00, 0x3e, 0x03, 0x55, 0xe4, 0x79, 0x09,
	0xe1, 0x5c, 0xd7, 0xd6, 0xb5, 0xc6, 0x6c, 0x1b, 0x0e, 0x53, 0x63, 0xfe, 0x14, 0x85, 0xc1, 0x86,
	0xa9, 0x00, 0xd3, 0x29, 0x42, 0xe0, 0x3c, 0x98, 0xa0, 0x9e, 0x3e, 0xb1, 0xae, 0x35, 0x26, 0x9d,
	0x09, 0xea, 0xc1, 0xcf, 0x1a, 0x58, 0xc6, 0xfd, 0x24, 0x21, 0x91, 0x70, 0x05, 0xc5, 0x5d, 0x77,
	0xa4, 0x5d, 0xbf, 0x23, 0xd9, 0x76, 0x2e, 0x53, 0xa3, 0xf2, 0x33, 0x35, 0x9e, 0xf8, 0x54, 0x1c,
	0xf5, 0x3b, 0x16, 0x66, 0xa1, 0xd2, 0xaf, 0x3e, 0x4d, 0xee, 0x75, 0x6d, 0x71, 0x1a, 0x13, 0x6e,
	0xbd, 0x21, 0x78, 0x98, 0x1a, 0x6b, 0x79, 0xee, 0xdb, 0x59, 0x4d, 0x67, 0x49, 0x01, 0xfb, 0x14,
	0x77, 0xdf, 0x15, 0x6e, 0xb8, 0x0c, 0xa6, 0x05, 0xeb, 0x92, 0xe8, 0xb9, 0x3e, 0x99, 0xa5, 0x75,
	0x94, 0x35, 0xf2, 0xb7, 0xf4, 0xa9, 0x92, 0xbf, 0x05, 0x7b, 0x00, 0x16, 0x09, 0x78, 0x2f, 0x11,
	0x6e, 0x9c, 0x50, 0x4c, 0xf4, 0x69, 0x29, 0xf9, 0xf5, 0xd8, 0x92, 0x17, 0x72, 0xc9, 0x3c, 0x66,
	0x8a, 0xc9, 0x74, 0xee, 0x29, 0xfa, 0xbd, 0x5e, 0x22, 0x76, 0x33, 0x17, 0x3c, 0x02, 0x73, 0xe5,
	0x9a, 0xf4, 0xaa, 0x4c, 0xf6, 0x76, 0x8c, 0x64, 0xdb, 0x91, 0x18, 0xa6, 0xc6, 0xe2, 0xdf, 0xfd,
	0x31, 0x9d, 0x5a, 0xa9, 0x2b, 0x70, 0x03, 0xcc, 0xc9, 0xae, 0xf1, 0x18, 0x61, 0x1a, 0xf9, 0xfa,
	0x4c, 0x36, 0xae, 0xf6, 0x83, 0x9b, 0xb3, 0x65, 0xd4, 0x74, 0x6a, 0x99, 0xb9, 0x97, 0x5b, 0xf0,
	0x5c, 0x03, 0x6b, 0x71, 0x42, 0x30, 0xe5, 0x94, 0x45, 0xee, 0x21, 0xc2, 0x82, 0x25, 0x2e, 0x52,
	0x65, 0xb9, 0x2c, 0x22, 0xfa, 0xac, 0xd4, 0xfd, 0x61, 0x6c, 0xdd, 0x8f, 0xf2, 0xdc, 0xff, 0x25,
	0x37, 0x9d, 0x95, 0x11, 0xbe, 0x25, 0xe1, 0xcd, 0xbc, 0x7b, 0x3b, 0x11, 0x81, 0x07, 0x60, 0x86,
	0x7f, 0x42, 0xb1, 0x7b, 0x48, 0x88, 0x0e, 0xa4, 0x88, 0xcd, 0xb1, 0x27, 0x75, 0x57, 0x4d, 0x4a,
	0xf1, 0x98, 0x4e, 0x35, 0xfb, 0xdd, 0x22, 0x04, 0x9e, 0x80, 0xfb, 0x01, 0xe2, 0xe2, 0xe6, 0xaa,
	0xb9, 0xfd, 0xd8, 0x43, 0x82, 0xe8, 0xb5, 0x75, 0xad, 0x51, 0x7b, 0xb1, 0x6a, 0xe5, 0x1b, 0x65,
	0x15, 0x1b, 0x65, 0xed, 0x17, 0x1b, 0xd5, 0x6e, 0x64, 0x32, 0x86, 0xa9, 0xf1, 0x30, 0x27, 0xbf,
	0x95, 0xc6, 0x3c, 0xfb, 0x65, 0x68, 0xce, 0x62, 0x86, 0x8d, 0x6e, 0xed, 0x7b, 0x89, 0x6c, 0x2c,
	0x7c, 0xb9, 0x30, 0x2a, 0xe7, 0x17, 0x46, 0xe5, 0xfb, 0xb7, 0xe6, 0x54, 0xb6, 0x93, 0xdb, 0xed,
	0x83, 0xcb, 0xeb, 0xba, 0x76, 0x75, 0x5d, 0xd7, 0x7e, 0x5f, 0xd7, 0xb5, 0xb3, 0x41, 0xbd, 0x72,
	0x35, 0xa8, 0x57, 0x7e, 0x0c, 0xea, 0x95, 0x8f, 0xed, 0x52, 0xa9, 0xea, 0xed, 0x68, 0x06, 0xa8,
	0xc3, 0x0b, 0xc3, 0x3e, 0x6e, 0xbd, 0xb2, 0x4f, 0xfe, 0xf5, 0xf2, 0x84, 0xcc, 0x23, 0x41, 0x67,
	0x5a, 0xd6, 0xf0, 0xf2, 0x4f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x2f, 0x45, 0x5e, 0xb2, 0xa8, 0x04,
	0x00, 0x00,
}

func (m *Pool) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Pool) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Pool) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	n1, err1 := github_com_gogo_protobuf_types.StdTimeMarshalTo(m.LastLiquidityUpdate, dAtA[i-github_com_gogo_protobuf_types.SizeOfStdTime(m.LastLiquidityUpdate):])
	if err1 != nil {
		return 0, err1
	}
	i -= n1
	i = encodeVarintPool(dAtA, i, uint64(n1))
	i--
	dAtA[i] = 0x5a
	{
		size := m.SwapFee.Size()
		i -= size
		if _, err := m.SwapFee.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintPool(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x52
	{
		size := m.PrecisionFactorAtPriceOne.Size()
		i -= size
		if _, err := m.PrecisionFactorAtPriceOne.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintPool(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x4a
	if m.TickSpacing != 0 {
		i = encodeVarintPool(dAtA, i, uint64(m.TickSpacing))
		i--
		dAtA[i] = 0x40
	}
	{
		size := m.CurrentTick.Size()
		i -= size
		if _, err := m.CurrentTick.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintPool(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x3a
	{
		size := m.CurrentSqrtPrice.Size()
		i -= size
		if _, err := m.CurrentSqrtPrice.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintPool(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x32
	if len(m.Token1) > 0 {
		i -= len(m.Token1)
		copy(dAtA[i:], m.Token1)
		i = encodeVarintPool(dAtA, i, uint64(len(m.Token1)))
		i--
		dAtA[i] = 0x2a
	}
	if len(m.Token0) > 0 {
		i -= len(m.Token0)
		copy(dAtA[i:], m.Token0)
		i = encodeVarintPool(dAtA, i, uint64(len(m.Token0)))
		i--
		dAtA[i] = 0x22
	}
	{
		size := m.CurrentTickLiquidity.Size()
		i -= size
		if _, err := m.CurrentTickLiquidity.MarshalTo(dAtA[i:]); err != nil {
			return 0, err
		}
		i = encodeVarintPool(dAtA, i, uint64(size))
	}
	i--
	dAtA[i] = 0x1a
	if m.Id != 0 {
		i = encodeVarintPool(dAtA, i, uint64(m.Id))
		i--
		dAtA[i] = 0x10
	}
	if len(m.Address) > 0 {
		i -= len(m.Address)
		copy(dAtA[i:], m.Address)
		i = encodeVarintPool(dAtA, i, uint64(len(m.Address)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintPool(dAtA []byte, offset int, v uint64) int {
	offset -= sovPool(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Pool) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Address)
	if l > 0 {
		n += 1 + l + sovPool(uint64(l))
	}
	if m.Id != 0 {
		n += 1 + sovPool(uint64(m.Id))
	}
	l = m.CurrentTickLiquidity.Size()
	n += 1 + l + sovPool(uint64(l))
	l = len(m.Token0)
	if l > 0 {
		n += 1 + l + sovPool(uint64(l))
	}
	l = len(m.Token1)
	if l > 0 {
		n += 1 + l + sovPool(uint64(l))
	}
	l = m.CurrentSqrtPrice.Size()
	n += 1 + l + sovPool(uint64(l))
	l = m.CurrentTick.Size()
	n += 1 + l + sovPool(uint64(l))
	if m.TickSpacing != 0 {
		n += 1 + sovPool(uint64(m.TickSpacing))
	}
	l = m.PrecisionFactorAtPriceOne.Size()
	n += 1 + l + sovPool(uint64(l))
	l = m.SwapFee.Size()
	n += 1 + l + sovPool(uint64(l))
	l = github_com_gogo_protobuf_types.SizeOfStdTime(m.LastLiquidityUpdate)
	n += 1 + l + sovPool(uint64(l))
	return n
}

func sovPool(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozPool(x uint64) (n int) {
	return sovPool(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Pool) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPool
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Pool: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Pool: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Address", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Address = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			m.Id = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Id |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentTickLiquidity", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CurrentTickLiquidity.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Token0", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Token0 = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Token1", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Token1 = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 6:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentSqrtPrice", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CurrentSqrtPrice.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 7:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CurrentTick", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.CurrentTick.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 8:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field TickSpacing", wireType)
			}
			m.TickSpacing = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.TickSpacing |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 9:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field PrecisionFactorAtPriceOne", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.PrecisionFactorAtPriceOne.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 10:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field SwapFee", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := m.SwapFee.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 11:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field LastLiquidityUpdate", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPool
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPool
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthPool
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := github_com_gogo_protobuf_types.StdTimeUnmarshal(&m.LastLiquidityUpdate, dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPool(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthPool
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipPool(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPool
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPool
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowPool
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthPool
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupPool
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthPool
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthPool        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPool          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupPool = fmt.Errorf("proto: unexpected end of group")
)