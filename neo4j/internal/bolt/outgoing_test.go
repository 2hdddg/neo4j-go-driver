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
	"bytes"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
	"reflect"
	"testing"
)

type testStruct struct {
	tag    byte
	fields []interface{}
}

// Utility to dehydrat/unpack
func unpack(u *packstream.Unpacker2) interface{} {
	u.Next()
	switch u.Curr {
	case packstream.PackedInt:
		return u.Int()
	case packstream.PackedFloat:
		return u.Float()
	case packstream.PackedStr:
		return u.String()
	case packstream.PackedByteArray:
		return u.ByteArray()
	case packstream.PackedNil:
		return nil
	case packstream.PackedTrue:
		return true
	case packstream.PackedFalse:
		return false
	case packstream.PackedArray:
		l := u.Len()
		a := make([]interface{}, l)
		for i := range a {
			a[i] = unpack(u)
		}
		return a
	case packstream.PackedMap:
		l := u.Len()
		m := make(map[string]interface{}, l)
		for i := uint32(0); i < l; i++ {
			u.Next()
			k := u.String()
			m[k] = unpack(u)
		}
		return m
	case packstream.PackedStruct:
		t := u.StructTag()
		l := u.Len()
		s := testStruct{tag: t}
		if l == 0 {
			return &s
		}
		s.fields = make([]interface{}, l)
		for i := range s.fields {
			s.fields[i] = unpack(u)
		}
		return &s
	default:
		panic(".")
	}
}

func TestOutgoing(ot *testing.T) {
	var err error
	out := &outgoing{
		chunker: newChunker(),
		packer:  &packstream.Packer2{},
		onErr:   func(e error) { err = e },
	}
	cases := []struct {
		name   string
		build  func()
		expect interface{}
	}{
		{
			name: "hello",
			build: func() {
				out.appendHello(nil)
			},
			expect: &testStruct{
				tag:    byte(msgHello),
				fields: []interface{}{map[string]interface{}{}},
			},
		},
		{
			name: "begin",
			build: func() {
				out.appendBegin(map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgBegin),
				fields: []interface{}{map[string]interface{}{"mode": "r"}},
			},
		},
		{
			name: "commit",
			build: func() {
				out.appendCommit()
			},
			expect: &testStruct{
				tag: byte(msgCommit),
			},
		},
		{
			name: "rollback",
			build: func() {
				out.appendRollback()
			},
			expect: &testStruct{
				tag: byte(msgRollback),
			},
		},
		{
			name: "goodbye",
			build: func() {
				out.appendGoodbye()
			},
			expect: &testStruct{
				tag: byte(msgGoodbye),
			},
		},
		{
			name: "reset",
			build: func() {
				out.appendReset()
			},
			expect: &testStruct{
				tag: byte(msgReset),
			},
		},
		{
			name: "pull all",
			build: func() {
				out.appendPullAll()
			},
			expect: &testStruct{
				tag: byte(msgPullAll),
			},
		},
		{
			name: "pull n",
			build: func() {
				out.appendPullN(7)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []interface{}{map[string]interface{}{"n": int64(7)}},
			},
		},
		{
			name: "pull n+qid",
			build: func() {
				out.appendPullNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgPullN),
				fields: []interface{}{map[string]interface{}{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "discard n+qid",
			build: func() {
				out.appendDiscardNQid(7, 10000)
			},
			expect: &testStruct{
				tag:    byte(msgDiscardN),
				fields: []interface{}{map[string]interface{}{"n": int64(7), "qid": int64(10000)}},
			},
		},
		{
			name: "run, no params, no meta",
			build: func() {
				out.appendRun("cypher", nil, nil)
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{}, map[string]interface{}{}},
			},
		},
		{
			name: "run, no params, meta",
			build: func() {
				out.appendRun("cypher", nil, map[string]interface{}{"mode": "r"})
			},
			expect: &testStruct{
				tag:    byte(msgRun),
				fields: []interface{}{"cypher", map[string]interface{}{}, map[string]interface{}{"mode": "r"}},
			},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			err = nil
			c.build()
			if err != nil {
				t.Fatal(err)
			}
			out.send(buf)

			// Dechunk it
			byts, err := dechunkMessage(buf, []byte{})
			if err != nil {
				t.Fatal(err)
			}
			// Hydrate it
			unpacker := &packstream.Unpacker2{}
			unpacker.Reset(byts)
			x := unpack(unpacker)
			if unpacker.Err != nil {
				t.Fatal(err)
			}
			// Validate it
			if !reflect.DeepEqual(x, c.expect) {
				t.Errorf("Unpacked differs, expected %+v (%T) but was %+v (%T)", c.expect, c.expect, x, x)
			}
		})
	}
}
