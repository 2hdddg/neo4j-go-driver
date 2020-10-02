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

package neo4j

import (
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/testutil"
)

type iter struct {
	expectNext   bool
	expectRec    *db.Record
	expectSum    *db.Summary
	expectSumErr error
	expectErr    error
}

func TestResult(ot *testing.T) {
	streamHandle := db.StreamHandle(0)
	cypher := ""
	params := map[string]interface{}{}
	recs := []*db.Record{
		&db.Record{},
		&db.Record{},
		&db.Record{},
	}
	sums := []*db.Summary{
		&db.Summary{},
	}
	errs := []error{
		errors.New("Whatever"),
	}

	// Initialization
	ot.Run("Initialization", func(t *testing.T) {
		conn := &testutil.ConnFake{}
		res := newResult(conn, streamHandle, cypher, params)
		rec := res.Record()
		if rec != nil {
			t.Errorf("Should be no record")
		}
		err := res.Err()
		if err != nil {
			t.Errorf("Should be no error")
		}
		if err != nil {
			t.Errorf("Shouldn't be an error to call summary too early")
		}
	})

	// Iterate without any unconsumed (no push from connection)
	iterCases := []struct {
		name   string
		stream []testutil.Next
		iters  []iter
		sum    db.Summary
	}{
		{
			name: "happy",
			stream: []testutil.Next{
				testutil.Next{Record: recs[0]},
				testutil.Next{Record: recs[1]},
				testutil.Next{Summary: sums[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: true, expectRec: recs[1]},
				iter{expectNext: false},
			},
		},
		{
			name: "error after one record",
			stream: []testutil.Next{
				testutil.Next{Record: recs[0]},
				testutil.Next{Err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
			},
		},
		{
			name: "proceed after error",
			stream: []testutil.Next{
				testutil.Next{Record: recs[0]},
				testutil.Next{Err: errs[0]},
			},
			iters: []iter{
				iter{expectNext: true, expectRec: recs[0]},
				iter{expectNext: false, expectErr: errs[0]},
				iter{expectNext: false, expectErr: errs[0]},
			},
		},
	}
	for _, c := range iterCases {
		ot.Run(fmt.Sprintf("Iteration-%s", c.name), func(t *testing.T) {
			conn := &testutil.ConnFake{Nexts: c.stream}
			res := newResult(conn, streamHandle, cypher, params)
			for i, call := range c.iters {
				gotNext := res.Next()
				if gotNext != call.expectNext {
					t.Fatalf("Next at iter %d returned %t but expected to return %t", i, gotNext, call.expectNext)
				}
				gotRec := res.Record()
				if (gotRec == nil) != (call.expectRec == nil) {
					if gotRec == nil {
						t.Fatalf("Expected to get record but didn't at iter %d", i)
					} else {
						t.Fatalf("Expected to NOT get a record but did at iter %d", i)
					}
				}
				gotErr := res.Err()
				if (gotErr == nil) != (call.expectErr == nil) {
					if gotErr == nil {
						t.Fatalf("Expected to get an error but didn't at iter %d", i)
					} else {
						t.Fatalf("Expected to NOT get an error but did at iter %d", i)
					}
				}
			}
		})
	}
}
