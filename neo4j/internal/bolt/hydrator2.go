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
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

var hydrationLenError = errors.New("Len assert fail")
var hydrationInvalidState = errors.New("Hydration state error")

type ignored struct{}
type success struct {
	fields        []string
	tfirst        int64
	qid           int64
	bookmark      string
	connectionId  string
	server        string
	db            string
	hasMore       bool
	tlast         int64
	qtype         db.StatementType
	counters      map[string]int
	plan          *db.Plan
	profile       *db.ProfiledPlan
	notifications []db.Notification
	num           uint32
}

func (s *success) summary() *db.Summary {
	return &db.Summary{
		Bookmark: s.bookmark,
	}
}

func (s *success) isResetResponse() bool {
	return s.num == 0
}

type hydrator struct {
	unpacker      packstream.Unpacker2
	unp           *packstream.Unpacker2
	err           error
	cachedIgnored ignored
	cachedSuccess success
}

func (h *hydrator) setErr(err error) {
	if h.err != nil {
		h.err = err
	}
}

func (h *hydrator) getErr() error {
	if h.unp.Err != nil {
		return h.unp.Err
	}
	return h.err
}

func (h *hydrator) assertLength(expect, actual uint32) {
	if expect != actual {
		h.setErr(errors.New(fmt.Sprintf(
			"Invalid length of struct, expected %d but was %d", expect, actual)))
	}
}

// hydrate hydrates a top-level struct message
func (h *hydrator) hydrate(buf []byte) (x interface{}, err error) {
	h.unp = &h.unpacker
	h.unp.Reset(buf)
	h.unp.Next()

	if h.unp.Curr != packstream.PackedStruct {
		return nil, errors.New(fmt.Sprintf("Unexpected curr: %d", h.unp.Curr))
	}

	n := h.unp.Len()
	t := packstream.StructTag(h.unp.StructTag())
	switch t {
	case msgSuccess:
		x = h.success(n)
	case msgIgnored:
		x = h.ignored(n)
	case msgFailure:
		x = h.failure(n)
	case msgRecord:
		x = h.record(n)
	default:
		h.setErr(errors.New(fmt.Sprintf("Unexpected tag at top level: %d", t)))
	}
	err = h.getErr()
	return
}

func (h *hydrator) ignored(n uint32) *ignored {
	h.assertLength(0, n)
	if h.getErr() != nil {
		return nil
	}
	return &h.cachedIgnored
}

func (h *hydrator) failure(n uint32) *db.Neo4jError {
	h.assertLength(1, n)
	if h.getErr() != nil {
		return nil
	}
	dberr := db.Neo4jError{}
	h.unp.Next() // Detect map
	for maplen := h.unp.Len(); maplen > 0; maplen-- {
		h.unp.Next()
		key := h.unp.String()
		h.unp.Next()
		switch key {
		case "code":
			dberr.Code = h.unp.String()
		case "message":
			dberr.Msg = h.unp.String()
		default:
			// Do not fail on unknown value in map
			h.trash()
		}
	}
	return &dberr
}

func (h *hydrator) success(n uint32) *success {
	h.assertLength(1, n)
	if h.getErr() != nil {
		return nil
	}
	// Use cached success
	succ := &h.cachedSuccess
	// Reset cached success
	succ.fields = succ.fields[:0]
	succ.bookmark = ""
	succ.hasMore = false
	succ.notifications = nil
	succ.plan = nil
	succ.profile = nil
	succ.qid = -1
	succ.server = ""
	h.unp.Next() // Detect map
	n = h.unp.Len()
	succ.num = n
	for ; n > 0; n-- {
		// Key
		h.unp.Next()
		key := h.unp.String()
		// Value
		h.unp.Next()
		switch key {
		case "fields":
			succ.fields = h.strings(succ.fields)
		case "t_first":
			succ.tfirst = h.unp.Int()
		case "qid":
			succ.qid = h.unp.Int()
		case "bookmark":
			succ.bookmark = h.unp.String()
		case "connection_id":
			succ.connectionId = h.unp.String()
		case "server":
			succ.server = h.unp.String()
		case "has_more":
			succ.hasMore = h.unp.Bool()
		case "t_last":
			succ.tlast = h.unp.Int()
		case "type":
			switch h.unp.String() {
			case "r":
				succ.qtype = db.StatementTypeRead
			case "w":
				succ.qtype = db.StatementTypeWrite
			case "rw":
				succ.qtype = db.StatementTypeReadWrite
			case "s":
				succ.qtype = db.StatementTypeSchemaWrite
			}
		case "stats":
			h.trash()
			//panic("stats not implemented")
		case "plan":
			h.trash()
			panic("plan not implemented")
		case "profile":
			h.trash()
			panic("profile not implemented")
		case "notifications":
			h.trash()
			panic("notifications not implemented")
		case "db":
			succ.db = h.unp.String()
		default:
			h.trash()
			panic("Unhandled key" + key)
			//return nil, hydrationInvalidState
		}
	}
	return succ
}

func (h *hydrator) strings(slice []string) []string {
	n := h.unp.Len()
	if slice == nil {
		slice = make([]string, 0, n)
	}
	for ; n > 0; n-- {
		h.unp.Next()
		slice = append(slice, h.unp.String())
	}
	return slice
}

func (h *hydrator) amap() (map[string]interface{}, error) {
	n := h.unp.Len()
	m := make(map[string]interface{}, n)
	var err error
	for ; n > 0; n-- {
		h.unp.Next()
		key := h.unp.String()
		h.unp.Next()
		m[key], err = h.value()

	}
	return m, err
}

func (h *hydrator) array() ([]interface{}, error) {
	n := h.unp.Len()
	a := make([]interface{}, n)
	for i := range a {
		h.unp.Next()
		a[i], _ = h.value()
	}
	return a, nil
}

func (h *hydrator) record(n uint32) *db.Record {
	h.assertLength(1, n)
	if h.getErr() != nil {
		return nil
	}
	rec := db.Record{}
	h.unp.Next() // Detect array
	n = h.unp.Len()
	rec.Values = make([]interface{}, n)
	for i := range rec.Values {
		h.unp.Next()
		rec.Values[i], _ = h.value()
	}
	return &rec
}

func (h *hydrator) value() (interface{}, error) {
	switch h.unp.Curr {
	case packstream.PackedInt:
		return h.unp.Int(), nil
	case packstream.PackedFloat:
		return h.unp.Float(), nil
	case packstream.PackedStr:
		return h.unp.String(), nil
	case packstream.PackedStruct:
		t := h.unp.StructTag()
		n := h.unp.Len()
		switch t {
		case 'N':
			return h.node(n)
		case 'R':
			return h.relationship(n)
		case 'r':
			return h.relationnode(n)
		case 'P':
			return h.path(n)
		case 'X':
			return h.point2d(n)
		case 'Y':
			return h.point3d(n)
		case 'F':
			return h.dateTimeOffset(n)
		case 'f':
			return h.dateTimeNamedZone(n)
		case 'd':
			return h.localDateTime(n)
		case 'D':
			return h.date(n)
		case 'T':
			return h.time(n)
		case 't':
			return h.localTime(n)
		case 'E':
			return h.duration(n)
		default:
			return nil, errors.New(fmt.Sprintf("Unknown tag: %02x", t))
		}
	case packstream.PackedByteArray:
		panic(1)
	case packstream.PackedArray:
		return h.array()
	case packstream.PackedMap:
		return h.amap()
	case packstream.PackedNil:
		return nil, nil
	case packstream.PackedTrue:
		return true, nil
	case packstream.PackedFalse:
		return false, nil
	default:
		return nil, hydrationInvalidState
	}
}

// Trashes current value
func (h *hydrator) trash() {
	// TODO Less consuming implementation
	h.value()
}

func (h *hydrator) node(num uint32) (interface{}, error) {
	var err error
	n := dbtype.Node{}
	h.unp.Next()
	n.Id = h.unp.Int()
	h.unp.Next()
	n.Labels = h.strings(nil)
	h.unp.Next()
	n.Props, err = h.amap()
	return n, err
}

func (h *hydrator) relationship(n uint32) (interface{}, error) {
	panic("")
}

func (h *hydrator) relationnode(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) path(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) point2d(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) point3d(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) dateTimeOffset(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) dateTimeNamedZone(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) localDateTime(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) date(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) time(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) localTime(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}

func (h *hydrator) duration(n uint32) (interface{}, error) {
	panic("")
	return nil, nil
}
