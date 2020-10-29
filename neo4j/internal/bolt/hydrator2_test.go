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
	//"errors"
	//"fmt"
	"reflect"
	"testing"
	//"time"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	//"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

func TestHydrator2(t *testing.T) {
	packer := packstream.Packer2{}
	cases := []struct {
		build func() ([]byte, error)
		x     interface{}
		err   error
	}{
		{
			build: func() ([]byte, error) {
				packer.Begin([]byte{})
				packer.StructHeader(byte(msgIgnored), 0)
				return packer.End()
			},
			x: &ignored{},
		},
		{
			build: func() ([]byte, error) {
				packer.Begin([]byte{})
				packer.StructHeader(byte(msgFailure), 0)
				packer.MapHeader(2)
				packer.String("code")
				packer.String("the code")
				packer.String("message")
				packer.String("mess")
				packer.String("extra key") // Should be ignored
				packer.Int(1)
				return packer.End()
			},
			x: &db.Neo4jError{Code: "the code", Msg: "mess"},
		},
		// TODO: Successes: empty, summary, summary with stats..., run, commit, hello, has more,
		// TODO: Records: temporal data types, spatial data types, maps of things
	}

	// Should be shared among calls in real usage
	hydrator := hydrator{}
	for _, c := range cases {
		buf, err := c.build()
		if err != nil {
			panic("Build error")
		}
		x, _ := hydrator.hydrate(buf)
		if !reflect.DeepEqual(x, c.x) {
			t.Fatalf("Expected: %+v != Actual: %+v", c.x, x)
		}
	}
}
