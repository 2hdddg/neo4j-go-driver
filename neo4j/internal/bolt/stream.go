package bolt

import (
	//"container/list"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
)

type streamPos int

const (
	pos_zero streamPos = iota
	pos_stream
	pos_page
	pos_discard // Not ended yet
	pos_end
)

type stream struct {
	pos       streamPos
	buffer    []*db.Record
	cursor    int
	keys      []string
	closed    bool
	qid       int
	fetchSize int
	sum       *db.Summary
	//err       error
}

func (s *stream) reset() {
	//s.err = nil
	s.closed = false
	s.cursor = 0
	s.keys = nil
	s.qid = -1
	s.fetchSize = -1
	s.sum = nil
}

// Returns a buffered record
func (s *stream) front() *db.Record {
	if cursor < len(s.buffer) {
		rec := s.buffer[cursor]
		s.cursor += 1
		return rec
	}
	return nil
}

// Adds  a record to the buffer
func (s *stream) pushBack(rec *db.Record) {
	if s.cursor > 0 && cap(s.buffer) == len(s.buffer) {
		copy(s.buffer, s.buffer[s.cursor:])
		s.cursor = 0
	}
	s.buffer = append(s.buffer, rec)
}

func (s *stream) close() {
	s.closed = true
	s.cursor = 0
}

func (s *stream) summary(sum *db.Summary) {
	s.sum = sum
	s.pos = pos_end
}

func (s *stream) page() {
	s.pos = pos_page
}
