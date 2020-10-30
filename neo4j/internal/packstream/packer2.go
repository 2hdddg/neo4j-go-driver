/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package packstream

import (
	"encoding/binary"
	"fmt"
	"math"
)

type Packer2 struct {
	buf []byte
	err error
}

func (p *Packer2) Begin(buf []byte) {
	p.buf = buf
	p.err = nil
}

func (p *Packer2) End() ([]byte, error) {
	return p.buf, p.err

}

func (p *Packer2) setErr(err error) {
	if p.err == nil {
		p.err = err
	}
}

func (p *Packer2) StructHeader(tag byte, num int) {
	if num > 0x0f {
		p.setErr(&OverflowError{msg: "Trying to pack struct with too many fields"})
		return
	}

	p.buf = append(p.buf, 0xb0+byte(num), byte(tag))
}

func (p *Packer2) Int64(i int64) {
	switch {
	case int64(-0x10) <= i && i < int64(0x80):
		p.buf = append(p.buf, byte(i))
	case int64(-0x80) <= i && i < int64(-0x10):
		p.buf = append(p.buf, 0xc8, byte(i))
	case int64(-0x8000) <= i && i < int64(0x8000):
		buf := [3]byte{0xc9}
		binary.BigEndian.PutUint16(buf[1:], uint16(i))
		p.buf = append(p.buf, buf[:]...)
	case int64(-0x80000000) <= i && i < int64(0x80000000):
		buf := [5]byte{0xca}
		binary.BigEndian.PutUint32(buf[1:], uint32(i))
		p.buf = append(p.buf, buf[:]...)
	default:
		buf := [9]byte{0xcb}
		binary.BigEndian.PutUint64(buf[1:], uint64(i))
		p.buf = append(p.buf, buf[:]...)
	}
}

func (p *Packer2) Int32(i int32) {
	p.Int64(int64(i))
}

func (p *Packer2) Int16(i int16) {
	p.Int64(int64(i))
}

func (p *Packer2) Int8(i int8) {
	p.Int64(int64(i))
}

func (p *Packer2) Int(i int) {
	p.Int64(int64(i))
}

func (p *Packer2) Uint64(i uint64) {
	p.checkOverflowInt(i)
	p.Int64(int64(i))
}

func (p *Packer2) Uint32(i uint32) {
	p.Int64(int64(i))
}

func (p *Packer2) Uint16(i uint16) {
	p.Int64(int64(i))
}

func (p *Packer2) Uint8(i uint8) {
	p.Int64(int64(i))
}

func (p *Packer2) Float64(f float64) {
	buf := [9]byte{0xc1}
	binary.BigEndian.PutUint64(buf[1:], math.Float64bits(f))
	p.buf = append(p.buf, buf[:]...)
}

func (p *Packer2) Float32(f float32) {
	p.Float64(float64(f))
}

func (p *Packer2) listHeader(ll int, shortOffset, longOffset byte) {
	l := int64(ll)
	hdr := make([]byte, 0, 1+4)
	if l < 0x10 {
		hdr = append(hdr, shortOffset+byte(l))
	} else {
		switch {
		case l < 0x100:
			hdr = append(hdr, []byte{longOffset, byte(l)}...)
		case l < 0x10000:
			hdr = hdr[:1+2]
			hdr[0] = longOffset + 1
			binary.BigEndian.PutUint16(hdr[1:], uint16(l))
		case l < math.MaxUint32:
			hdr = hdr[:1+4]
			hdr[0] = longOffset + 2
			binary.BigEndian.PutUint32(hdr[1:], uint32(l))
		default:
			p.err = &OverflowError{msg: fmt.Sprintf("Trying to pack too large list of size %d ", l)}
			return
		}
	}
	p.buf = append(p.buf, hdr...)
}

func (p *Packer2) String(s string) {
	p.listHeader(len(s), 0x80, 0xd0)
	p.buf = append(p.buf, []byte(s)...)
}

func (p *Packer2) ArrayHeader(l int) {
	p.listHeader(l, 0x90, 0xd4)
}

func (p *Packer2) MapHeader(l int) {
	p.listHeader(l, 0xa0, 0xd8)
}

func (p *Packer2) Bytes(b []byte) {
	hdr := make([]byte, 0, 1+4)
	l := int64(len(b))
	switch {
	case l < 0x100:
		hdr = append(hdr, 0xcc, byte(l))
	case l < 0x10000:
		hdr = hdr[:1+2]
		hdr[0] = 0xcd
		binary.BigEndian.PutUint16(hdr[1:], uint16(l))
	case l < 0x100000000:
		hdr = hdr[:1+4]
		hdr[0] = 0xce
		binary.BigEndian.PutUint32(hdr[1:], uint32(l))
	default:
		p.err = &OverflowError{msg: fmt.Sprintf("Trying to pack too large byte array of size %d", l)}
		return
	}
	p.buf = append(p.buf, hdr...)
	p.buf = append(p.buf, b...)
}

func (p *Packer2) Bool(b bool) {
	if b {
		p.buf = append(p.buf, 0xc3)
		return
	}
	p.buf = append(p.buf, 0xc2)
}

func (p *Packer2) Nil() {
	p.buf = append(p.buf, 0xc0)
}

func (p *Packer2) checkOverflowInt(i uint64) {
	if i > math.MaxInt64 {
		p.err = &OverflowError{msg: "Trying to pack uint64 that doesn't fit into int64"}
	}
}
