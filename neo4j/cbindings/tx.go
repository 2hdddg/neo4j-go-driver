package main

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/retry"
)

type tx struct {
	conn db.Connection
	dbtx db.TxHandle
	ret  func(db.Connection)
	pool *pool.Pool
	retr *retry.State
}

func (t *tx) commit() error {
	err := t.conn.TxCommit(t.dbtx)
	if err != nil && t.retr != nil {
		t.retr.OnFailure(t.conn, err, true)
	}
	t.pool.Return(t.conn)
	return err
}

func (t *tx) rollback() error {
	err := t.conn.TxRollback(t.dbtx)
	if err != nil && t.retr != nil {
		t.retr.OnFailure(t.conn, err, false)
	}
	t.pool.Return(t.conn)
	return err
}

func (t *tx) stream(cypher string, params map[string]interface{}) (*stream, error) {
	cmd := db.Command{Cypher: cypher, Params: params, FetchSize: 10}
	dbstream, err := t.conn.RunTx(t.dbtx, cmd)
	if err != nil {
		if t.retr != nil {
			t.retr.OnFailure(t.conn, err, false)
		}
		return nil, err
	}
	return &stream{
		conn:     t.conn,
		dbstream: dbstream,
	}, nil
}
