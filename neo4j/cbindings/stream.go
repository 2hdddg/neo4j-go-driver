package main

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type stream struct {
	conn     db.Connection
	dbstream db.StreamHandle
	rec      *db.Record
}

func (s *stream) next() (bool, error) {
	rec, sum, err := s.conn.Next(s.dbstream)
	if err != nil {
		return false, err
	}
	if sum != nil {
		return false, nil
	}

	s.rec = rec
	return true, nil
}
