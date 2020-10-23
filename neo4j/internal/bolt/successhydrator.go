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
	//"fmt"
	//"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	//"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

const (
	succ_reset = iota
	succ_rootmap
	succ_fields
	succ_done
)

type successResponse struct {
	fields       []string
	tfirst       int64
	tlast        int64
	qid          int64
	bookmark     string
	connectionId string
	server       string
	hasMore      bool
	stmnttype    db.StatementType
	num          int
}

func (s successResponse) summary() *db.Summary {
	return &db.Summary{Bookmark: s.bookmark, StmntType: s.stmnttype, TLast: s.tlast}
}

type successHydrator struct {
	err   error
	state int
	key   string
	resp  successResponse
}

func (h *successHydrator) reset() {
	h.err = nil
	h.state = succ_reset
	h.key = ""
	h.resp.fields = h.resp.fields[:0]
	h.resp.num = 0
}

func (h *successHydrator) GetHydrator(tag packstream.StructTag, n uint32) (packstream.Hydrator, error) {
	h.err = errors.New("No nested struct in success message")
	return nil, h.err
}

func (h *successHydrator) OnString(s string) {
	switch h.state {
	case succ_rootmap:
		switch h.key {
		case "":
			h.key = s
		case "type":
			switch s {
			case "r":
				h.resp.stmnttype = db.StatementTypeRead
			case "w":
				h.resp.stmnttype = db.StatementTypeWrite
			case "rw":
				h.resp.stmnttype = db.StatementTypeReadWrite
			case "s":
				h.resp.stmnttype = db.StatementTypeSchemaWrite
			}
			h.resp.num++
		case "bookmark":
			h.resp.bookmark = s
			h.resp.num++
		case "connection_id":
			h.resp.connectionId = s
			h.resp.num++
		case "server":
			h.resp.server = s
			h.resp.num++
		}
	case succ_fields:
		h.resp.fields = append(h.resp.fields, s)
	}
}

func (h *successHydrator) OnInt(i int64) {
	switch h.state {
	case succ_rootmap:
		switch h.key {
		case "qid":
			h.resp.qid = i
			h.resp.num++
		case "t_first":
			h.resp.tfirst = i
			h.resp.num++
		case "t_last":
			h.resp.tlast = i
			h.resp.num++
		}
	}
}

func (h *successHydrator) OnBool(b bool) {
	switch h.state {
	case succ_rootmap:
		switch h.key {
		case "has_more":
			h.resp.hasMore = b
			h.resp.num++
		}
	}
}

func (h *successHydrator) OnMapBegin(n uint32) {
	switch h.state {
	case succ_reset:
		h.state = succ_rootmap
	default:
		h.err = errors.New("Unexpected map")
	}
}

func (h *successHydrator) OnMapEnd() {
	switch h.state {
	case succ_rootmap:
		h.state = succ_done
	default:
		h.err = errors.New("Unexpected end of map")
	}
}

func (h *successHydrator) OnArrayBegin(n uint32) {
	switch h.state {
	case succ_rootmap:
		switch h.key {
		case "fields":
			h.state = succ_fields
		}
	}
}

func (h *successHydrator) OnArrayEnd() {
	switch h.state {
	case succ_fields:
		h.state = succ_rootmap
	}
}

func (h *successHydrator) OnFloat(f float64) {}
func (h *successHydrator) OnBytes(b []byte)  {}
func (h *successHydrator) OnNil()            {}

func (h *successHydrator) End() (interface{}, error) {
	return &h.resp, h.err
}

