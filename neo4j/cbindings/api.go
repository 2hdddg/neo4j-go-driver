package main

import (
	"errors"
	"unsafe"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/retry"
)

/*
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct {
	const char *desc;
} neo4j_error;

typedef struct {
	const char *uri; // TODO: auth info
} neo4j_driverconfig;

typedef struct {
	const char *bookmark;
	uint32_t   timeout_ms;
	bool       retry;
} neo4j_txconfig;

typedef struct {
	uint8_t    typ;
	int64_t    val; // bool, ints, floats
	void*      ref; // map, array, node, path, relationship, temporal, spatial
} neo4j_value;

typedef struct {
	const char  *name;
	uint8_t     typ;
	int64_t     val; // bool, ints, floats
	void*       ref; // map, array, node, path, relationship, temporal, spatial
} neo4j_param;

typedef struct {
	int         num;
	neo4j_param *params;
} neo4j_command;

typedef struct {
	const char *bookmark;
} neo4j_commit;

typedef uint32_t neo4j_handle;

typedef struct {
	int         num;
	char        **keys;
	neo4j_value *values;
} neo4j_map;

typedef struct {
	int         num;
	neo4j_value *values;
} neo4j_array;

typedef enum {
	NEO4J_UNDEF,
	NEO4J_NULL,
	NEO4J_INT64,
} cyphertypes;



*/
import "C"

var (
	drivers map[uint32]*driver
	txs     map[uint32]*tx
	streams map[uint32]*stream
	retries map[uint32]*retry.State
	handle  uint32
)

func nextHandle() uint32 {
	handle += 1
	return handle
}

//export neo4j_driver_create
func neo4j_driver_create(
	config *C.neo4j_driverconfig,
	handle_out *C.neo4j_handle,
	err_out **C.neo4j_error) C.int {

	neo4j_err_free(err_out)

	// Initialize driver instance
	driver, err := newDriver(C.GoString(config.uri))
	if err != nil {
		err_set(err, err_out)
		return 0
	}

	// Stash the driver instance and return handle
	handle := nextHandle()
	drivers[handle] = driver
	*handle_out = C.uint32_t(handle)
	return 1
}

//export neo4j_driver_destroy
func neo4j_driver_destroy(driver_handle C.neo4j_handle) {

	// Retrieve the driver instance
	driver := drivers[uint32(driver_handle)]
	if driver == nil {
		return
	}

	// Close driver and unstash it
	driver.close()
	drivers[uint32(driver_handle)] = nil
}

//export neo4j_driver_tx
//
// Requests a new transaction on the driver instance.
//
func neo4j_driver_tx(
	driver_handle C.neo4j_handle,
	config *C.neo4j_txconfig,
	tx_handle_out *C.neo4j_handle,
	retry_handle_inout *C.neo4j_handle,
	err_out **C.neo4j_error) C.int {

	neo4j_err_free(err_out)

	// Retrieve the driver instance
	driver := drivers[uint32(driver_handle)]
	if driver == nil {
		err_set(errors.New("Invalid driver handle"), err_out)
		return 0
	}

	// Handle retry, if requested.
	var retr *retry.State
	stashed_retry := true
	if retry_handle_inout != nil {
		retry_handle := *retry_handle_inout
		if retry_handle == 0 {
			retr = driver.newRetry()
			stashed_retry = false
		} else {
			retr = retries[uint32(driver_handle)]
		}
	}

	// Start a new transaction
	tx, err := driver.newTx(retr)
	if err != nil {
		err_set(err, err_out)
		return 0
	}

	// Stash the retry context if it is new
	if !stashed_retry {
		handle := nextHandle()
		retries[handle] = retr
		*retry_handle_inout = C.uint32_t(handle)
	}

	// Stash the transaction
	handle := nextHandle()
	txs[handle] = tx
	*tx_handle_out = C.uint32_t(handle)

	return 1
}

//export neo4j_tx_commit
func neo4j_tx_commit(tx_handle C.neo4j_handle, commit_out *C.neo4j_commit, err_out **C.neo4j_error) C.int {

	neo4j_err_free(err_out)

	// Retrieve the transaction
	tx := txs[uint32(tx_handle)]
	if tx == nil {
		err_set(errors.New("Invalid tx handle"), err_out)
		return 0
	}

	// Unstash the transaction
	delete(txs, uint32(tx_handle))

	// Commit
	err := tx.commit()
	if err != nil {
		err_set(err, err_out)
		return 0
	}

	return 1
}

//export neo4j_tx_rollback
func neo4j_tx_rollback(tx_handle C.neo4j_handle, err_out **C.neo4j_error) C.int {

	neo4j_err_free(err_out)

	// Retrieve the transaction
	tx := txs[uint32(tx_handle)]
	if tx == nil {
		err_set(errors.New("Invalid tx handle"), err_out)
		return 0
	}

	// Unstash the transaction
	delete(txs, uint32(tx_handle))

	// Rollback
	err := tx.rollback()
	if err != nil {
		err_set(err, err_out)
		return 0
	}

	return 1
}

//export neo4j_tx_stream
func neo4j_tx_stream(tx_handle C.neo4j_handle, cypher *C.char, num_params C.int, cparams *C.neo4j_param, handle_out *C.neo4j_handle, err_out **C.neo4j_error) C.int {
	tx := txs[uint32(tx_handle)]
	if tx == nil {
		// TODO: Set err_out
		return 0
	}

	// Convert C parameters to Go parameters
	n := int(num_params)
	params := make(map[string]interface{}, n)
	s := (*[1 << 30]C.neo4j_param)(unsafe.Pointer(cparams))[:n:n]
	for _, x := range s {
		switch x.typ {
		case C.NEO4J_INT64:
			params[C.GoString(x.name)] = int64(x.val)
		}
	}

	stream, err := tx.stream(C.GoString(cypher), params)
	if err != nil {
		return 0
	}

	handle := nextHandle()
	*handle_out = C.uint32_t(handle)
	streams[handle] = stream
	return 1
}

//export neo4j_stream_next
func neo4j_stream_next(stream_handle C.neo4j_handle, err_out **C.neo4j_error) C.int {
	stream := streams[uint32(stream_handle)]
	more, err := stream.next()
	if err != nil {
		return 0
	}
	if !more {
		return 0
	}
	return 1
}

//export neo4j_stream_value
// Values can be resued between calls without freeing them, if the previous value is compatible with
// the new value, memory can be reused without allocation (reallocation can occur).
func neo4j_stream_value(stream_handle C.neo4j_handle, cindex C.int, value_out *C.neo4j_value, err_out **C.neo4j_error) C.int {
	stream := streams[uint32(stream_handle)]
	if stream == nil {
		return 0
	}
	if stream.rec == nil {
		return 0
	}
	index := int(cindex)
	if index >= len(stream.rec.Values) {
		return 0
	}
	switch v := stream.rec.Values[index].(type) {
	case int64:
		value_out.typ = C.NEO4J_INT64
		value_out.val = C.int64_t(v)
		return 1
	}

	return 0
}

//export neo4j_retry_free
func neo4j_retry_free(retry_handle C.neo4j_handle) {
	retr := retries[uint32(retry_handle)]
	if retr != nil {
		delete(retries, uint32(retry_handle))
	}
}

//export neo4j_retry
func neo4j_retry(retry_handle C.neo4j_handle) C.int {
	retr := retries[uint32(retry_handle)]
	if retr != nil {
		if !retr.Stop {
			return 1
		}
	}
	return 0
}

//export neo4j_err_free
func neo4j_err_free(err **C.neo4j_error) {
	if err != nil {
		if *err != nil {
			// Free it
		}
		*err = nil
	}
}

func err_set(err error, err_out **C.neo4j_error) {
	if err_out != nil {
		//p := (*C.neo4j_error)C.malloc(C.sizeof_neo4j_error)
		//*err_out = p
	}
}

//export neo4j_value_free
func neo4j_value_free(value *C.neo4j_value) {
}

func main() {
}

func init() {
	drivers = make(map[uint32]*driver)
	txs = make(map[uint32]*tx)
	streams = make(map[uint32]*stream)
	retries = make(map[uint32]*retry.State)
}

// NOTES:
/// All functions that take neo4j_error should:
//    check if there is an error before executing, if so abort
//    upon own error, close the connection, remove the transaction, set the error

/*

go build -buildmode c-shared -o libneo4j.so .


*/
