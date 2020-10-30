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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
	"reflect"
	"testing"
	"time"
)

func TestHydrator2(ot *testing.T) {
	zone := "America/New_York"
	loc, err := time.LoadLocation(zone)
	if err != nil {
		panic(err)
	}

	packer := packstream.Packer2{}
	cases := []struct {
		name  string
		build func()      // Builds/encodes stream same was as server would
		x     interface{} // Expected hydrated
		err   error
	}{
		{
			name: "Ignored",
			build: func() {
				packer.StructHeader(byte(msgIgnored), 0)
			},
			x: &ignored{},
		},
		{
			name: "Error",
			build: func() {
				packer.StructHeader(byte(msgFailure), 0)
				packer.MapHeader(3)
				packer.String("code")
				packer.String("the code")
				packer.String("message")
				packer.String("mess")
				packer.String("extra key") // Should be ignored
				packer.Int(1)
			},
			x: &db.Neo4jError{Code: "the code", Msg: "mess"},
		},
		{
			name: "Success hello response",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(3)
				packer.String("connection_id")
				packer.String("connid")
				packer.String("server")
				packer.String("srv")
				packer.String("details") // Should be ignored
				packer.Int8(1)
			},
			x: &success{connectionId: "connid", server: "srv", qid: -1, num: 3},
		},
		{
			name: "Success commit/rollback/reset response",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(0)
			},
			x: &success{qid: -1, num: 0},
		},
		{
			name: "Success run response",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(3)
				packer.String("unknown") // Should be ignored
				packer.Int64(666)
				packer.String("fields")
				packer.ArrayHeader(2)   // >> fields array
				packer.String("field1") //
				packer.String("field2") // << fields array
				packer.String("t_first")
				packer.Int64(10000)
			},
			x: &success{fields: []string{"field1", "field2"}, tfirst: 10000, qid: -1, num: 3},
		},
		{
			name: "Success discard/end of page response with more data",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(1)
				packer.String("has_more")
				packer.Bool(true)
			},
			x: &success{hasMore: true, qid: -1, num: 1},
		},
		{
			name: "Success discard response with no more data",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(4)
				packer.String("has_more")
				packer.Bool(false)
				packer.String("whatever") // >> Whatever array to ignore
				packer.ArrayHeader(2)     //
				packer.Int(1)             //
				packer.Int(2)             // << Whatever array
				packer.String("bookmark")
				packer.String("bm")
				packer.String("db")
				packer.String("sys")
			},
			x: &success{bookmark: "bm", db: "sys", qid: -1, num: 4},
		},
		{
			name: "Success pull response, write with db",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(4)
				packer.String("bookmark")
				packer.String("b")
				packer.String("t_last")
				packer.Int64(124)
				packer.String("type")
				packer.String("w")
				packer.String("db")
				packer.String("s")
			},
			x: &success{tlast: 124, bookmark: "b", qtype: db.StatementTypeWrite, db: "s", qid: -1, num: 4},
		},
		{
			name: "Success pull response read no db",
			build: func() {
				packer.StructHeader(byte(msgSuccess), 1)
				packer.MapHeader(4)
				packer.String("bookmark")
				packer.String("b1")
				packer.String("t_last")
				packer.Int64(7)
				packer.String("type")
				packer.String("r")
				packer.String("has_more")
				packer.Bool(false)
			},
			x: &success{tlast: 7, bookmark: "b1", qtype: db.StatementTypeRead, qid: -1, num: 4},
		}, // TODO: Successes: , summary with stats...,
		{
			name: "Record of ints",
			build: func() {
				packer.StructHeader(byte(msgRecord), 1)
				packer.ArrayHeader(5)
				packer.Int(1)
				packer.Int(2)
				packer.Int(3)
				packer.Int(4)
				packer.Int(5)
			},
			x: &db.Record{Values: []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}},
		},
		{
			name: "Record of spatials",
			build: func() {
				packer.StructHeader(byte(msgRecord), 1)
				packer.ArrayHeader(2)
				packer.StructHeader('X', 3) // Point2D
				packer.Int64(1)             //
				packer.Float64(7.123)       //
				packer.Float64(123.7)       //
				packer.StructHeader('Y', 4) // Point3D
				packer.Int64(2)             //
				packer.Float64(0.123)       //
				packer.Float64(23.71)       //
				packer.Float64(3.712)       //
			},
			x: &db.Record{Values: []interface{}{
				dbtype.Point2D{SpatialRefId: 1, X: 7.123, Y: 123.7},
				dbtype.Point3D{SpatialRefId: 2, X: 0.123, Y: 23.71, Z: 3.712},
			}},
		},
		{
			name: "Record of temporals",
			build: func() {
				packer.StructHeader(byte(msgRecord), 1)
				packer.ArrayHeader(7)
				// Time
				packer.StructHeader('T', 2)
				packer.Int64(int64(time.Hour*1 + time.Minute*2 + time.Second*3 + 4))
				packer.Int64(6)
				// Local time
				packer.StructHeader('t', 1)
				packer.Int64(int64(time.Hour*1 + time.Minute*2 + time.Second*3 + 4))
				// Date
				packer.StructHeader('D', 1)
				packer.Int64(time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC).Unix() / (60 * 60 * 24))
				// Datetime, local
				packer.StructHeader('d', 2)
				t := time.Date(1999, 12, 31, 23, 59, 59, 1, time.UTC)
				packer.Int64(t.Unix())
				packer.Int64(t.UnixNano() - (t.Unix() * int64(time.Second)))
				// Datetime, named zone
				packer.StructHeader('f', 3)
				t = time.Date(1999, 12, 31, 23, 59, 59, 1, time.UTC)
				packer.Int64(t.Unix())
				packer.Int64(t.UnixNano() - (t.Unix() * int64(time.Second)))
				packer.String(zone)
				// Datetime, offset zone
				packer.StructHeader('F', 3)
				t = time.Date(1999, 12, 31, 23, 59, 59, 1, time.UTC)
				packer.Int64(t.Unix())
				packer.Int64(t.UnixNano() - (t.Unix() * int64(time.Second)))
				packer.Int(3)
				// Duration
				packer.StructHeader('E', 4)
				packer.Int64(12)
				packer.Int64(31)
				packer.Int64(59)
				packer.Int64(10001)
			},
			x: &db.Record{Values: []interface{}{
				dbtype.Time(time.Date(0, 0, 0, 1, 2, 3, 4, time.FixedZone("Offset", 6))),
				dbtype.LocalTime(time.Date(0, 0, 0, 1, 2, 3, 4, time.Local)),
				dbtype.Date(time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)),
				dbtype.LocalDateTime(time.Date(1999, 12, 31, 23, 59, 59, 1, time.Local)),
				time.Date(1999, 12, 31, 23, 59, 59, 1, loc),
				time.Date(1999, 12, 31, 23, 59, 59, 1, time.FixedZone("Offset", 3)),
				dbtype.Duration{Months: 12, Days: 31, Seconds: 59, Nanos: 10001},
			}},
		},

		// TODO: Records: temporal data types, maps of things, node, path, relationship
	}

	// Shared among calls in real usage so we do the same while testing it.
	hydrator := hydrator{}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			packer.Begin([]byte{})
			c.build()
			buf, err := packer.End()
			if err != nil {
				panic("Build error")
			}
			x, err := hydrator.hydrate(buf)
			if err != nil {
				panic(err)
			}
			if !reflect.DeepEqual(x, c.x) {
				t.Fatalf("Expected:\n%+v\n != Actual: \n%+v\n", c.x, x)
			}
		})
	}
}
