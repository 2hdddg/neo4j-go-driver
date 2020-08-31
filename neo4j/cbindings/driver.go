package main

import (
	"context"
	"net/url"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/connector"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/pool"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/retry"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/log"
)

type driver struct {
	log       log.Logger
	connector connector.Connector
	pool      *pool.Pool
	uri       *url.URL
}

func newDriver(uri string) (*driver, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if parsed.Port() == "" {
		parsed.Host = parsed.Host + ":7687"
	}

	d := &driver{
		uri: parsed,
		log: &log.Console{Errors: true, Infos: true, Warns: true, Debugs: true},
	}
	// TODO:
	d.connector.SkipEncryption = true
	d.connector.Auth = map[string]interface{}{"scheme": "basic", "principal": "neo4j", "credentials": "pass"}
	d.connector.Network = "tcp"
	d.connector.Log = d.log
	d.pool = pool.New(10, time.Second*60, d.connector.Connect, d.log, "poolid")
	return d, nil
}

func (d *driver) close() {
	if d == nil {
		return
	}
	d.pool.Close()
}

func (d *driver) newRetry() *retry.State {
	return &retry.State{
		Log:                d.log,
		LogName:            "x",
		LogId:              "y",
		Now:                time.Now,
		Sleep:              time.Sleep,
		Throttle:           retry.Throttler(time.Second * 1),
		MaxDeadConnections: 10,
		Router:             nil,
	}
}

func (d *driver) newTx(retr *retry.State) (*tx, error) {
	for retr == nil || retr.Continue() {
		conn, err := d.pool.Borrow(context.Background(), []string{d.uri.Host}, true)
		if err != nil {
			if retr == nil {
				return nil, err
			}
			retr.OnFailure(conn, err, false)
			continue
		}

		// TODO: params
		conf := db.TxConfig{Mode: db.ReadMode, Timeout: 10 * time.Second}
		dbtx, err := conn.TxBegin(conf)
		if err != nil {
			if retr == nil {
				d.pool.Return(conn)
				return nil, err
			}
			retr.OnFailure(conn, err, false)
			d.pool.Return(conn)
			continue
		}

		return &tx{
			pool: d.pool,
			conn: conn,
			ret:  d.pool.Return,
			dbtx: dbtx,
			retr: retr,
		}, nil
	}
	return nil, nil
}
