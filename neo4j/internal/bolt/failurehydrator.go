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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	//"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

type failureHydrator struct {
	maplevel int
	key      string
	err      error
	code     string
	msg      string
}

func (h *failureHydrator) GetHydrator(tag packstream.StructTag, n uint32) (packstream.Hydrator, error) {
	h.err = hydrationError
	return nil, h.err
}

func (h *failureHydrator) OnString(s string) {
	switch h.maplevel {
	case 1:
		switch h.key {
		case "":
			h.key = s
		case "code":
			h.code = s
		case "message":
			h.msg = s
		}
	}
}

func (h *failureHydrator) reset() {
	h.maplevel = 0
	h.key = ""
}

func (h *failureHydrator) OnInt(i int64) {}

func (h *failureHydrator) OnBool(b bool) {}

func (h *failureHydrator) OnMapBegin(n uint32) {
	h.maplevel++
}

func (h *failureHydrator) OnMapEnd() {
	h.maplevel--
}

func (h *failureHydrator) OnArrayBegin(n uint32) {}

func (h *failureHydrator) OnArrayEnd() {}

func (h *failureHydrator) OnFloat(f float64) {}
func (h *failureHydrator) OnBytes(b []byte)  {}
func (h *failureHydrator) OnNil()            {}

func (h *failureHydrator) End() (interface{}, error) {
	return &db.Neo4jError{Code: h.code, Msg: h.msg}, h.err
}
