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

// Called by unpacker to let receiver check all fields and let the receiver return an
// hydrated instance or an error if any of the fields or number of fields are different
// expected. Values from fields must be copied, the fields slice cannot be saved.
type Hydrate func(tag StructTag, fields []interface{}) (interface{}, error)

type Hydrator interface {
	GetHydrator(tag StructTag, n uint32) (Hydrator, error)
	OnInt(i int64)
	OnBool(b bool)
	OnFloat(f float64)
	OnString(s string)
	OnBytes(b []byte) // Need to be copied by hydrator
	OnNil()
	OnArrayBegin(n uint32)
	OnArrayEnd()
	OnMapBegin(n uint32)
	OnMapEnd()
	End() (interface{}, error)
}

type Unpacker struct {
	buf    []byte
	offset uint32
	length uint32
	err    error
	fields []interface{} // Cached field list for unpacking structs faster
	foff   int           // Offset into fields when nesting structs
}

func (u *Unpacker) pop() byte {
	if u.offset < u.length {
		x := u.buf[u.offset]
		u.offset += 1
		return x
	}
	u.err = &IoError{}
	return 0
}

func (u *Unpacker) read(n uint32) []byte {
	start := u.offset
	end := u.offset + n
	if end > u.length {
		u.err = &IoError{}
		return nil
	}
	u.offset = end
	return u.buf[start:end]
}

func (u *Unpacker) readStruct(hydrator Hydrator, n uint32) (interface{}, error) {
	if n > 0x0f {
		u.err = &IllegalFormatError{msg: fmt.Sprintf("Invalid struct size: %d", n)}
		return nil, u.err
	}

	// Read struct tag and retrieve a hydrator for it
	tag := StructTag(u.pop())
	hydrator, err := hydrator.GetHydrator(tag, n)
	if err != nil {
		u.err = err
		return nil, u.err
	}

	// Read each field in the struct and add it to the hydrator
	for i := uint32(0); i < n; i++ {
		u.unpack(hydrator)
	}

	if u.err != nil {
		return nil, u.err
	}
	return hydrator.End()

	/*
		// No need for allocating when no fields
		if numFields == 0 && u.err == nil {
			return hydrate(tag, nil)
		}

			// Use cached fields slice and grow it if needed
			if len(u.fields) < (u.foff + numFields) {
				newFields := make([]interface{}, len(u.fields)+numFields)
				copy(newFields, u.fields)
				u.fields = newFields
			}

			// Read fields and pass them to hydrator
			foff := u.foff
			u.foff += numFields
			for i := foff; i < numFields+foff; i++ {
				u.fields[i], _ = u.unpack(hydrate)
			}
			u.foff -= numFields
			if u.err != nil {
				return nil, u.err
			}
			fields := u.fields[u.foff : u.foff+numFields]
			return hydrate(tag, fields)
	*/
}

func (u *Unpacker) readStr(n uint32) string {
	buf := u.read(n)
	if u.err != nil {
		return ""
	}
	return string(buf)
}

func (u *Unpacker) readArr(hydrator Hydrator, n uint32) {
	//arr := make([]interface{}, n)
	for i := uint32(0); i < n; i++ {
		u.unpack(hydrator)
	}
	/*
		for i := range arr {
			arr[i], _ = u.unpack(hydrate)
		}
		return arr, u.err
	*/
}

func (u *Unpacker) readMap(hydrator Hydrator, n uint32) {
	for i := uint32(0); i < n; i++ {
		// Hydrate key and value
		u.unpack(hydrator)
		u.unpack(hydrator)
	}

	/*
		m := make(map[string]interface{}, n)
		for i := uint32(0); i < n; i++ {
			keyx, _ := u.unpack(hydrate)
			key, ok := keyx.(string)
			if !ok {
				if u.err == nil {
					u.err = &IllegalFormatError{msg: fmt.Sprintf("Map key is not string type: %T", keyx)}
				}
				return nil, u.err
			}
			valx, _ := u.unpack(hydrate)
			m[key] = valx
		}

		return m, u.err
	*/
}

func (u *Unpacker) unpack(hydrator Hydrator) {
	// Read field marker
	marker := u.pop()
	if u.err != nil {
		return
	}

	switch {
	case marker < 0x80:
		// Tiny positive int
		hydrator.OnInt(int64(marker))
	case marker == 0x80:
		// Empty string
		hydrator.OnString("")
	case marker < 0x90:
		// Tiny string
		hydrator.OnString(u.readStr(uint32(marker) - 0x80))
	case marker < 0xa0:
		// Tiny array
		n := uint32(marker - 0x90)
		hydrator.OnArrayBegin(n)
		u.readArr(hydrator, n)
		hydrator.OnArrayEnd()
	case marker < 0xb0:
		// Tiny map
		n := uint32(marker - 0xa0)
		hydrator.OnMapBegin(n)
		u.readMap(hydrator, n)
		hydrator.OnMapEnd()
	case marker < 0xc0:
		// Struct
		n := uint32(marker - 0xc0)
		u.readStruct(hydrator, n)
	case marker == 0xc0:
		// Nil
		hydrator.OnNil()
	case marker == 0xc1:
		// Float
		buf := u.read(8)
		if u.err == nil {
			f := math.Float64frombits(binary.BigEndian.Uint64(buf))
			hydrator.OnFloat(f)
		}
	case marker == 0xc2:
		// False
		hydrator.OnBool(false)
	case marker == 0xc3:
		// True
		hydrator.OnBool(true)
	case marker == 0xc8:
		// Int, 1 byte
		hydrator.OnInt(int64(int8(u.pop())))
	case marker == 0xc9:
		// Int, 2 bytes
		buf := u.read(2)
		if u.err == nil {
			hydrator.OnInt(int64(int16(binary.BigEndian.Uint16(buf))))
		}
	case marker == 0xca:
		// Int, 4 bytes
		buf := u.read(4)
		if u.err == nil {
			hydrator.OnInt(int64(int32(binary.BigEndian.Uint32(buf))))
		}
	case marker == 0xcb:
		// Int, 8 bytes
		buf := u.read(8)
		if u.err == nil {
			hydrator.OnInt(int64(binary.BigEndian.Uint64(buf)))
		}
	case marker == 0xcb:
		// Int, 8 bytes
		buf := u.read(8)
		if u.err == nil {
			hydrator.OnInt(int64(binary.BigEndian.Uint64(buf)))
		}
	case marker == 0xcc:
		// byte[] of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return
		}
		hydrator.OnBytes(u.read(uint32(num)))
	case marker == 0xcd:
		// byte[] of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint16(buf)
		hydrator.OnBytes(u.read(uint32(num)))
	case marker == 0xce:
		// byte[] of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint32(buf)
		hydrator.OnBytes(u.read(num))
	case marker == 0xd0:
		// String of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return
		}
		hydrator.OnString(u.readStr(uint32(num)))
	case marker == 0xd1:
		// String of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint16(buf)
		hydrator.OnString(u.readStr(uint32(num)))
	case marker == 0xd2:
		// String of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint32(buf)
		hydrator.OnString(u.readStr(num))
	case marker == 0xd4:
		// Array of length up to 0xff
		num := u.pop()
		if u.err != nil {
			return
		}
		hydrator.OnArrayBegin(uint32(num))
		u.readArr(hydrator, uint32(num))
		hydrator.OnArrayEnd()
	case marker == 0xd5:
		// Array of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint16(buf)
		hydrator.OnArrayBegin(uint32(num))
		u.readArr(hydrator, uint32(num))
		hydrator.OnArrayEnd()
	case marker == 0xd6:
		// Array of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return
		}
		num := binary.BigEndian.Uint32(buf)
		hydrator.OnArrayBegin(uint32(num))
		u.readArr(hydrator, uint32(num))
		hydrator.OnArrayEnd()
	case marker == 0xd8:
		// Map of length up to 0xff
		n := uint32(u.pop())
		if u.err != nil {
			return
		}
		hydrator.OnMapBegin(n)
		u.readMap(hydrator, n)
		hydrator.OnMapEnd()
	case marker == 0xd9:
		// Map of length up to 0xffff
		buf := u.read(2)
		if u.err != nil {
			return
		}
		n := uint32(binary.BigEndian.Uint16(buf))
		hydrator.OnMapBegin(n)
		u.readMap(hydrator, n)
		hydrator.OnMapEnd()
	case marker == 0xda:
		// Map of length up to 0xffffffff
		buf := u.read(4)
		if u.err != nil {
			return
		}
		n := binary.BigEndian.Uint32(buf)
		hydrator.OnMapBegin(n)
		u.readMap(hydrator, n)
		hydrator.OnMapEnd()
	}

	/*
		if marker < 0x80 {
			// Tiny positive int
			return int64(marker), nil
		}
		if marker > 0x80 && marker < 0x90 {
			// Tiny string
			return u.readStr(uint32(marker) - 0x80)
		}
		if marker > 0x90 && marker < 0xa0 {
			// Tiny array
			return u.readArr(hydrate, uint32(marker-0x90))
		}
		if marker >= 0xf0 {
			// Tiny negative int
			return int64(marker) - 0x100, nil
		}
		if marker > 0xa0 && marker < 0xb0 {
			// Tiny map
			return u.readMap(hydrate, uint32(marker-0xa0))
		}
		if marker >= 0xb0 && marker < 0xc0 {
			return u.readStruct(hydrate, int(marker-0xb0))
		}
	*/

	//switch marker {
	//case 0x80:
	//	// Empty string
	//	return "", nil
	//case 0x90:
	//	// Empty array
	//	return []interface{}{}, nil
	//case 0xa0:
	//	// Empty map
	//	return map[string]interface{}{}, nil
	//case 0xc0:
	//	// Nil
	//	return nil, nil
	/*
		case 0xc1:
			// Float
			buf := u.read(8)
			if u.err != nil {
				return nil, u.err
			}
			return math.Float64frombits(binary.BigEndian.Uint64(buf)), nil
		case 0xc2:
			// False
			return false, nil
		case 0xc3:
			// True
			return true, nil
		case 0xc8:
			// Int, 1 byte
			return int64(int8(u.pop())), u.err
		case 0xc9:
			// Int, 2 bytes
			buf := u.read(2)
			if u.err != nil {
				return nil, u.err
			}
			return int64(int16(binary.BigEndian.Uint16(buf))), nil
		case 0xca:
			// Int, 4 bytes
			buf := u.read(4)
			if u.err != nil {
				return nil, u.err
			}
			return int64(int32(binary.BigEndian.Uint32(buf))), nil
		case 0xcb:
			// Int, 8 bytes
			buf := u.read(8)
			if u.err != nil {
				return nil, u.err
			}
			return int64(binary.BigEndian.Uint64(buf)), nil
		case 0xcc:
			// byte[] of length up to 0xff
			num := u.pop()
			if u.err != nil {
				return nil, u.err
			}
			return u.read(uint32(num)), u.err
		case 0xcd:
			// byte[] of length up to 0xffff
			buf := u.read(2)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint16(buf)
			return u.read(uint32(num)), u.err
		case 0xce:
			// byte[] of length up to 0xffffffff
			buf := u.read(4)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint32(buf)
			return u.read(num), u.err
		case 0xd0:
			// String of length up to 0xff
			num := u.pop()
			if u.err != nil {
				return nil, u.err
			}
			return u.readStr(uint32(num))
		case 0xd1:
			// String of length up to 0xffff
			buf := u.read(2)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint16(buf)
			return u.readStr(uint32(num))
		case 0xd2:
			// String of length up to 0xffffffff
			buf := u.read(4)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint32(buf)
			return u.readStr(num)
		case 0xd4:
			// Array of length up to 0xff
			num := u.pop()
			if u.err != nil {
				return nil, u.err
			}
			return u.readArr(hydrate, uint32(num))
		case 0xd5:
			// Array of length up to 0xffff
			buf := u.read(2)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint16(buf)
			return u.readArr(hydrate, uint32(num))
		case 0xd6:
			// Array of length up to 0xffffffff
			buf := u.read(4)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint32(buf)
			return u.readArr(hydrate, num)
		case 0xd8:
			// Map of length up to 0xff
			num := u.pop()
			if u.err != nil {
				return nil, u.err
			}
			return u.readMap(hydrate, uint32(num))
		case 0xd9:
			// Map of length up to 0xffff
			buf := u.read(2)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint16(buf)
			return u.readMap(hydrate, uint32(num))
		case 0xda:
			// Map of length up to 0xffffffff
			buf := u.read(4)
			if u.err != nil {
				return nil, u.err
			}
			num := binary.BigEndian.Uint32(buf)
			return u.readMap(hydrate, num)
		}

		return nil, &IllegalFormatError{msg: fmt.Sprintf("Unknown marker: %02x", marker)}
	*/
}

func (u *Unpacker) Unpack(hydrator Hydrator, buf []byte) (interface{}, error) {
	// Reset
	u.buf = buf
	u.offset = 0
	u.length = uint32(len(buf))
	u.err = nil
	u.foff = 0

	n := u.pop() - 0xb0
	if u.err != nil {
		return nil, u.err
	}

	return u.readStruct(hydrator, uint32(n))
}

/*
func (u *Unpacker) UnpackStruct(buf []byte, hydrate Hydrate) (interface{}, error) {
	u.buf = buf
	u.offset = 0
	u.length = uint32(len(buf))
	u.err = nil
	u.foff = 0

	numFields := u.pop() - 0xb0
	if u.err != nil {
		return nil, u.err
	}
	return u.readStruct(hydrate, int(numFields))
}
*/
