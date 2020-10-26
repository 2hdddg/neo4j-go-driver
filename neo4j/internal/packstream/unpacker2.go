package packstream

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Type byte

const (
	PackedUndef = iota // Undefined must be zero!
	PackedInt
	PackedFloat
	PackedStr
	PackedStruct
	PackedByteArray
	PackedArray
	PackedMap
	PackedNil
	PackedTrue
	PackedFalse
)

type Unpacker2 struct {
	buf  []byte
	off  uint32
	len  uint32
	mrk  marker
	Err  error
	Curr int // Packed type
}

func (u *Unpacker2) Reset(buf []byte) {
	u.buf = buf
	u.off = 0
	u.len = uint32(len(buf))
	u.Err = nil
	u.mrk.typ = PackedUndef
	u.Curr = PackedUndef
}

func (u *Unpacker2) setErr(err error) {
	if u.Err == nil {
		u.Err = err
	}
}

func (u *Unpacker2) Next() {
	i := u.pop()
	u.mrk = markers[i]
	u.Curr = u.mrk.typ
}

func (u *Unpacker2) Len() uint32 {
	n := u.mrk.numlenbytes
	if n > 0 {
		switch n {
		case 1:
			return uint32(u.pop())
		case 2:
			buf := u.read(2)
			if u.Err != nil {
				return 0
			}
			return uint32(binary.BigEndian.Uint16(buf))
		case 4:
			buf := u.read(4)
			if u.Err != nil {
				return 0
			}
			return binary.BigEndian.Uint32(buf)
		default:
			u.setErr(&UnpackError{msg: fmt.Sprintf("Illegal length: %d (%d)", n, u.Curr)})
			return 0
		}
	}
	return uint32(u.mrk.shortlen)
}

func (u *Unpacker2) Int() int64 {
	n := u.mrk.numlenbytes
	if n > 0 {
		switch n {
		case 1:
			return int64(int8(u.pop()))
		case 2:
			buf := u.read(2)
			if u.Err != nil {
				return 0
			}
			return int64(int16(binary.BigEndian.Uint16(buf)))
		case 4:
			buf := u.read(4)
			if u.Err != nil {
				return 0
			}
			return int64(int32(binary.BigEndian.Uint32(buf)))
		case 8:
			// Int, 8 bytes
			buf := u.read(8)
			if u.Err != nil {
				return 0
			}
			return int64(binary.BigEndian.Uint64(buf))
		default:
			u.setErr(&UnpackError{msg: fmt.Sprintf("Illegal int length: %d", n)})
			return 0
		}
	}
	return int64(u.mrk.shortlen)
}

func (u *Unpacker2) Float() float64 {
	buf := u.read(8)
	if u.Err != nil {
		return math.NaN()
	}
	return math.Float64frombits(binary.BigEndian.Uint64(buf))
}

func (u *Unpacker2) StructTag() byte {
	return u.pop()
}

func (u *Unpacker2) String() string {
	buf := u.read(u.Len())
	if u.Err != nil {
		return ""
	}
	return string(buf)
}

func (u *Unpacker2) Bool() bool {
	switch u.Curr {
	case PackedTrue:
		return true
	case PackedFalse:
		return false
	default:
		u.setErr(&UnpackError{msg: "Illegal value for bool"})
	}
	return false
}

func (u *Unpacker2) ByteArray() []byte {
	n := u.Len()
	buf := u.read(n)
	if u.Err != nil || n == 0 {
		return nil
	}
	out := make([]byte, n)
	copy(out, buf)
	return out
}

func (u *Unpacker2) pop() byte {
	if u.off < u.len {
		x := u.buf[u.off]
		u.off += 1
		return x
	}
	u.setErr(&IoError{})
	return 0
}

func (u *Unpacker2) read(n uint32) []byte {
	start := u.off
	end := u.off + n
	if end > u.len {
		u.setErr(&IoError{})
		return nil
	}
	u.off = end
	return u.buf[start:end]
}

type marker struct {
	typ         int
	shortlen    byte
	numlenbytes byte
}

var markers [0xff]marker

func init() {
	i := 0
	// Tiny int
	for ; i < 0x80; i++ {
		markers[i] = marker{typ: PackedInt, shortlen: byte(i)}
	}
	// Tiny string
	for ; i < 0x90; i++ {
		markers[i] = marker{typ: PackedStr, shortlen: byte(i - 0x80)}
	}
	// Tiny array
	for ; i < 0xa0; i++ {
		markers[i] = marker{typ: PackedArray, shortlen: byte(i - 0x90)}
	}
	// Tiny map
	for ; i < 0xb0; i++ {
		markers[i] = marker{typ: PackedMap, shortlen: byte(i - 0xa0)}
	}
	// Struct
	for ; i < 0xc0; i++ {
		markers[i] = marker{typ: PackedStruct, shortlen: byte(i - 0xb0)}
	}

	markers[0xc0] = marker{typ: PackedNil}
	markers[0xc1] = marker{typ: PackedFloat, numlenbytes: 8}
	markers[0xc2] = marker{typ: PackedFalse}
	markers[0xc3] = marker{typ: PackedTrue}

	markers[0xc8] = marker{typ: PackedInt, numlenbytes: 1}
	markers[0xc9] = marker{typ: PackedInt, numlenbytes: 2}
	markers[0xca] = marker{typ: PackedInt, numlenbytes: 4}
	markers[0xcb] = marker{typ: PackedInt, numlenbytes: 8}

	markers[0xcc] = marker{typ: PackedByteArray, numlenbytes: 1}
	markers[0xcd] = marker{typ: PackedByteArray, numlenbytes: 2}
	markers[0xce] = marker{typ: PackedByteArray, numlenbytes: 4}

	markers[0xd0] = marker{typ: PackedStr, numlenbytes: 1}
	markers[0xd1] = marker{typ: PackedStr, numlenbytes: 2}
	markers[0xd2] = marker{typ: PackedStr, numlenbytes: 4}

	markers[0xd4] = marker{typ: PackedArray, numlenbytes: 1}
	markers[0xd5] = marker{typ: PackedArray, numlenbytes: 2}
	markers[0xd6] = marker{typ: PackedArray, numlenbytes: 4}

	markers[0xd8] = marker{typ: PackedMap, numlenbytes: 1}
	markers[0xd9] = marker{typ: PackedMap, numlenbytes: 2}
	markers[0xda] = marker{typ: PackedMap, numlenbytes: 4}
}
