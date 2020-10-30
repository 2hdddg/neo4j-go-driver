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

package bolt

import (
	"fmt"
	"net"
	"reflect"

	//"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

type outgoing struct {
	chunker *chunker
	packer  *packstream.Packer2
	onErr   func(err error)
}

func (o *outgoing) _begin() {
	o.chunker.beginMessage()
	o.packer.Begin(o.chunker.buf)
}

func (o *outgoing) _end() {
	buf, err := o.packer.End()
	o.chunker.buf = buf
	o.chunker.endMessage()
	if err != nil {
		o.onErr(err)
	}
}

func (o *outgoing) appendHello(hello map[string]interface{}) {
	o._begin()
	o.packer.StructHeader(byte(msgHello), 1)
	o._packMap(hello)
	o._end()
}

func (o *outgoing) appendBegin(meta map[string]interface{}) {
	o._begin()
	o.packer.StructHeader(byte(msgBegin), 1)
	o._packMap(meta)
	o._end()
}

func (o *outgoing) appendCommit() {
	o._begin()
	o.packer.StructHeader(byte(msgCommit), 0)
	o._end()
}

func (o *outgoing) appendRollback() {
	o._begin()
	o.packer.StructHeader(byte(msgRollback), 0)
	o._end()
}

func (o *outgoing) appendRun(cypher string, params, meta map[string]interface{}) {
	o._begin()
	o.packer.StructHeader(byte(msgRun), 3)
	o.packer.String(cypher)
	o._packMap(params)
	o._packMap(meta)
	o._end()
}

func (o *outgoing) appendPullN(n int) {
	o._begin()
	o.packer.StructHeader(byte(msgPullN), 1)
	o.packer.MapHeader(1)
	o.packer.String("n")
	o.packer.Int(n)
	o._end()
}

func (o *outgoing) appendPullNQid(n int, qid int64) {
	o._begin()
	o.packer.StructHeader(byte(msgPullN), 1)
	o.packer.MapHeader(2)
	o.packer.String("n")
	o.packer.Int(n)
	o.packer.String("qid")
	o.packer.Int64(qid)
	o._end()
}

func (o *outgoing) appendDiscardNQid(n int, qid int64) {
	o._begin()
	o.packer.StructHeader(byte(msgDiscardN), 1)
	o.packer.MapHeader(2)
	o.packer.String("n")
	o.packer.Int(n)
	o.packer.String("qid")
	o.packer.Int64(qid)
	o._end()
}

func (o *outgoing) appendPullAll() {
	o._begin()
	o.packer.StructHeader(byte(msgPullAll), 0)
	o._end()
}

func (o *outgoing) appendReset() {
	o._begin()
	o.packer.StructHeader(byte(msgReset), 0)
	o._end()
}

func (o *outgoing) appendGoodbye() {
	o._begin()
	o.packer.StructHeader(byte(msgGoodbye), 0)
	o._end()
}

func (o *outgoing) send(conn net.Conn) {
	err := o.chunker.send(conn)
	if err != nil {
		o.onErr(err)
	}
}

func (o *outgoing) _packMap(m map[string]interface{}) {
	o.packer.MapHeader(len(m))
	for k, v := range m {
		o.packer.String(k)
		o._packX(v)
	}
}

func (o *outgoing) _packX(x interface{}) {
	if x == nil {
		o.packer.Nil()
		return
	}
	v := reflect.ValueOf(x)
	switch v.Kind() {
	case reflect.Bool:
		o.packer.Bool(v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		o.packer.Int64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		o.packer.Uint32(uint32(v.Uint()))
	case reflect.Uint64:
		o.packer.Uint64(v.Uint())
	case reflect.Float32, reflect.Float64:
		o.packer.Float64(v.Float())
	case reflect.String:
		o.packer.String(v.String())
	case reflect.Ptr:
		if v.IsNil() {
			o.packer.Nil()
			return
		}
		// Inspect what the pointer points to
		i := reflect.Indirect(v)
		switch i.Kind() {
		case reflect.Struct:
			panic("Todo")
			/*
				s, isS := x.(*Struct)
				if isS {
					p.writeStruct(s.Tag, s.Fields)
				} else {
					// Unknown type, call dehydration hook to make it into a struct
					p.tryDehydrate(x)
				}
			*/
		default:
			panic("Todo")
			//p.pack(i.Interface())
		}
	case reflect.Struct:
		// Unknown type, call dehydration hook to make it into a struct
		//p.tryDehydrate(x)
		panic("Todo")
	case reflect.Slice:
		num := v.Len()
		o.packer.ArrayHeader(num)
		for i := 0; i < num; i++ {
			o._packX(v.Index(i).Interface())
		}
	case reflect.Map:
		t := reflect.TypeOf(x)
		if t.Key().Kind() != reflect.String {
			o.onErr(&UnsupportedTypeError{t: reflect.TypeOf(x)})
			return
		}
		o.packer.MapHeader(v.Len())
		// TODO Use MapRange when min Go version is >= 1.12
		for _, ki := range v.MapKeys() {
			o.packer.String(ki.String())
			o._packX(v.MapIndex(ki).Interface())
		}
	default:
		o.onErr(&UnsupportedTypeError{t: reflect.TypeOf(x)})
	}
}

type UnsupportedTypeError struct {
	t reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	return fmt.Sprintf("Packing of type '%s' is not supported", e.t.String())
}
