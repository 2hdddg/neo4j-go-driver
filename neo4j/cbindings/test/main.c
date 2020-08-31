
#include "../libneo4j.h"
#include <stdio.h>

// Command part of retryable transaction
int run_command(neo4j_handle txhandle, neo4j_error **txerr) {
    neo4j_handle streamhandle;
    neo4j_param params[] = {
        { name: "param1", typ: NEO4J_INT64, val: 777 }
    };
    if (!neo4j_tx_stream(txhandle, "RETURN $param1 AS x", 1, params, &streamhandle, txerr)) {
        return 0;
    }

    neo4j_value xval = {};
    while (neo4j_stream_next(streamhandle, txerr)) {
        if (!neo4j_stream_value(streamhandle, 0, &xval, txerr)) {
            break;
        }

        switch (xval.typ) {
        case NEO4J_INT64:
            printf("Got x: %d, %ld\n", xval.typ, xval.val);
            break;
        default:
            printf("Got something unexpected\n");
            break;
        }
    }
    neo4j_value_free(&xval);
    return *txerr == NULL ? 1 :0;
}

int run_retryable_tx(neo4j_handle driverhandle) {
    neo4j_txconfig txconfig = {};
    neo4j_error *err = NULL;
    neo4j_handle txhandle = 0;
    neo4j_handle retryhandle = 0;
    neo4j_commit commit = {};

    while (retryhandle == 0 || neo4j_retry(retryhandle)) {
        // Creates a new transaction and a retry state
        // Transaction is bound to retry state, all operations
        // within the transaction will report any errors to retry state.
        if (!neo4j_driver_tx(driverhandle, &txconfig, &txhandle, &retryhandle, &err)) {
            //neo4j_retry_free(retryhandle);
            neo4j_err_free(&err);
            return 0;
        }

        if (run_command(txhandle, &err) &&
            run_command(txhandle, &err) &&
            neo4j_tx_commit(txhandle, &commit, &err)) {
            // No error to free and commit command frees the associated
            // retry state upon success.
            return 1;
        }

        neo4j_err_free(&err);

        /*
        if (!(run_command(txhandle, &err) &&
              run_command(txhandle, &err))) {
            if (!neo4j_retry(retryhandle)) {
                neo4j_err_free(&err);
                neo4j_tx_rollback(txhandle, NULL);
                neo4j_retry_free(retryhandle);
                return 0;
            } else {
                continue;
            }
        }

        if (!neo4j_tx_commit(txhandle, &commit, &err)) {
            // No need to call neo4j_tx_rollback, the transaction is gone.
            if (!neo4j_retry(retryhandle)) {
                neo4j_retry_free(retryhandle);
                neo4j_err_free(&err);
                return 0;
            } else {
                continue;
            }
        }

        return 1;
        */
    }
}

int run_tx(neo4j_handle driverhandle) {
    neo4j_txconfig txconfig = {};
    neo4j_error *err = NULL;
    neo4j_handle txhandle = 0;
    neo4j_commit commit = {};

    if (!neo4j_driver_tx(driverhandle, &txconfig, &txhandle, NULL, &err)) {
        printf("Failed to create tx\n");
        neo4j_err_free(&err);
        return 0;
    }

    if (!(run_command(txhandle, &err) &&
          run_command(txhandle, &err))) {
        neo4j_err_free(&err);
        neo4j_tx_rollback(txhandle, NULL);
        return 0;
    }

    if (!neo4j_tx_commit(txhandle, &commit, &err)) {
        neo4j_err_free(&err);
        return 0;
    }
    return 1;
}

int main() {
    neo4j_driverconfig driverconfig = {
        uri: "neo4j://localhost"
    };
    neo4j_txconfig txconfig = {};
    neo4j_handle driverhandle = 0;
    neo4j_handle txhandle = 0;
    neo4j_error *err = NULL;

    if (!neo4j_driver_create(&driverconfig, &driverhandle, &err)) {
        neo4j_err_free(&err);
        return -1;
    }

    if (!run_tx(driverhandle)) {
        printf("run_tx failed\n");
        return -1;
    }

    if (!run_retryable_tx(driverhandle)) {
        printf("run_retryable_tx failed\n");
        return -1;
    }
    neo4j_driver_destroy(driverhandle);
}

// gcc -o main main.c -L .. -lneo4j
//
// LD_LIBRARY_PATH=.. ./main
