/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tgtdb

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"

	"github.com/yugabyte/yb-voyager/yb-voyager/src/utils"
)

var defaultSessionVars = []string{
	"SET client_encoding to 'UTF-8'",
	"SET session_replication_role to replica",
}

type ConnectionParams struct {
	NumConnections    int
	NumMaxConnections int
	ConnUriList       []string
	SessionInitScript []string
}

/*
ConnectionPool is a pool of connections to a YugabyteDB cluster.
It has for the following features:
 1. Re-use of connections. Connections are not closed after a query is executed.
    They are returned to the pool, unless there is an error.
 2. Load balancing across multiple YugabyteDB nodes. Connections are created in a round-robin fashion.
 3. Prepared statements are cached per connection
 4. Dynamic pool size. The pool size can be increased or decreased
*/
type ConnectionPool struct {
	sync.Mutex
	params *ConnectionParams
	conns  chan *pgx.Conn
	// in adaptive parallelism, we may want to reduce the pool size, but
	// we would not want to close the connection, as we might need to increase the pool
	// size in the future. So, we move the connections to idleConns instead.
	idleConns                 chan *pgx.Conn
	connIdToPreparedStmtCache map[uint32]map[string]bool // cache list of prepared statements per connection
	nextUriIndex              int
	disableThrottling         bool
	size                      int // current size of the pool
	// in adaptive parallelism, we may want to reduce the pool size, but
	// doing it synchronously may lead to contention. So, we increment the
	// counter pendingConnsToClose, and close the connections asynchronously.
	pendingConnsToClose     int
	pendingConnsToCloseLock sync.Mutex
}

func NewConnectionPool(params *ConnectionParams) *ConnectionPool {
	pool := &ConnectionPool{
		params:                    params,
		conns:                     make(chan *pgx.Conn, params.NumMaxConnections),
		idleConns:                 make(chan *pgx.Conn, params.NumMaxConnections),
		connIdToPreparedStmtCache: make(map[uint32]map[string]bool, params.NumMaxConnections),
		disableThrottling:         false,
		size:                      params.NumConnections,
		pendingConnsToClose:       0,
	}
	for i := 0; i < params.NumMaxConnections; i++ {
		pool.idleConns <- nil
	}
	for i := 0; i < params.NumConnections; i++ {
		c := <-pool.idleConns
		pool.conns <- c
	}
	if pool.params.SessionInitScript == nil {
		pool.params.SessionInitScript = defaultSessionVars
	}
	return pool
}

func (pool *ConnectionPool) GetNumConnections() int {
	return pool.size
}

func (pool *ConnectionPool) UpdateNumConnections(delta int) error {
	pool.pendingConnsToCloseLock.Lock()
	defer pool.pendingConnsToCloseLock.Unlock()

	newSize := pool.size + delta

	if newSize < 1 || newSize > pool.params.NumMaxConnections {
		return fmt.Errorf("invalid new pool size %d. "+
			"Must be between 1 and %d", newSize, pool.params.NumMaxConnections)
	}

	if delta == 0 {
		log.Infof("adaptive: No change in pool size. Current size is %d", pool.size)
		return nil
	}
	if delta > 0 {
		// for increases, process them synchronously.
		// If there is non-zero pendingConnsToClose, then we reduce that count.
		if pool.pendingConnsToClose >= 0 {
			// since we are increasing, we can just reduce the pending close count
			pendingConnsToNotClose := min(pool.pendingConnsToClose, delta)
			log.Infof("adaptive: Decreasing pendingConnsToClose by %d", pendingConnsToNotClose)
			pool.pendingConnsToClose -= pendingConnsToNotClose
			delta -= pendingConnsToNotClose
		}
		// Additionally, pick conns from the idle pool, and add it to the main pool.
		for i := 0; i < delta; i++ {
			conn := <-pool.idleConns
			pool.conns <- conn
		}
		log.Infof("adaptive: Added %d new connections. Pool size is now %d", delta, newSize)
	} else {
		// for decreases, process them asynchronously.
		// The problem with processing them synchronously is that we may have to wait for the
		// connections to be returned to the pool. Not only that, this goroutine that is trying to
		// update the pool size by reading from the channel is also competing with multiple
		// other goroutines that are trying to ingest data.
		//  This might take significant time depending on the query execution time and the lock contention
		//  So, instead, we just register the request to close the connections,
		// and the connections are returned to the idle pool when the query execution is done.
		pool.pendingConnsToClose += -delta
		log.Infof("adaptive: registered request to close %d conns. Total pending conns to close=%d", -delta, pool.pendingConnsToClose)
	}
	pool.size = newSize
	return nil
}

func (pool *ConnectionPool) DisableThrottling() {
	pool.disableThrottling = true
}

func (pool *ConnectionPool) WithConn(fn func(*pgx.Conn) (bool, error)) error {
	var err error
	retry := true

	for retry {
		var conn *pgx.Conn
		var gotIt bool
		if pool.disableThrottling {
			conn = <-pool.conns
		} else {
			conn, gotIt = <-pool.conns
			if !gotIt {
				// The following sleep is intentional. It is added so that voyager does not
				// overwhelm the database. See the description in PR https://github.com/yugabyte/yb-voyager/pull/920 .
				time.Sleep(2 * time.Second)
				continue
			}
		}
		if conn == nil {
			conn, err = pool.createNewConnection()
			if err != nil {
				return err
			}
		}

		retry, err = fn(conn)
		if err != nil {
			// On err, drop the connection and clear the prepared statement cache.
			conn.Close(context.Background())
			pool.Lock()
			// assuming PID will still be available
			delete(pool.connIdToPreparedStmtCache, conn.PgConn().PID())
			pool.Unlock()

			pool.pendingConnsToCloseLock.Lock()
			if pool.pendingConnsToClose > 0 {
				pool.idleConns <- nil
				log.Infof("adaptive: Closed and moved connection to idle pool because pendingConnsToClose = %d", pool.pendingConnsToClose)
				pool.pendingConnsToClose--
			} else {
				pool.conns <- nil
			}
			pool.pendingConnsToCloseLock.Unlock()
		} else {
			pool.pendingConnsToCloseLock.Lock()
			if pool.pendingConnsToClose > 0 {
				pool.idleConns <- conn
				log.Infof("adaptive: Moved connection to idle pool because pendingConnsToClose = %d. main connection pool size=%d", pool.pendingConnsToClose, len(pool.conns))
				pool.pendingConnsToClose--
			} else {
				pool.conns <- conn
			}
			pool.pendingConnsToCloseLock.Unlock()
		}
	}

	return err
}

func (pool *ConnectionPool) PrepareStatement(conn *pgx.Conn, stmtName string, stmt string) error {
	if pool.isStmtAlreadyPreparedOnConn(conn.PgConn().PID(), stmtName) {
		return nil
	}

	_, err := conn.Prepare(context.Background(), stmtName, stmt)
	if err != nil {
		log.Errorf("failed to prepare statement %q: %s", stmtName, err)
		return fmt.Errorf("failed to prepare statement %q: %w", stmtName, err)
	}
	pool.cachePreparedStmtForConn(conn.PgConn().PID(), stmtName)
	return err
}

func (pool *ConnectionPool) cachePreparedStmtForConn(connId uint32, ps string) {
	pool.Lock()
	defer pool.Unlock()
	if pool.connIdToPreparedStmtCache[connId] == nil {
		pool.connIdToPreparedStmtCache[connId] = make(map[string]bool)
	}
	pool.connIdToPreparedStmtCache[connId][ps] = true
}

func (pool *ConnectionPool) isStmtAlreadyPreparedOnConn(connId uint32, ps string) bool {
	pool.Lock()
	defer pool.Unlock()
	if pool.connIdToPreparedStmtCache[connId] == nil {
		return false
	}
	return pool.connIdToPreparedStmtCache[connId][ps]
}

func (pool *ConnectionPool) createNewConnection() (*pgx.Conn, error) {
	idx := pool.getNextUriIndex()
	uri := pool.params.ConnUriList[idx]
	conn, err := pool.connect(uri)
	if err != nil {
		for _, uri := range pool.shuffledConnUriList() {
			conn, err = pool.connect(uri)
			if err == nil {
				break
			}
		}
	}
	return conn, err
}

func (pool *ConnectionPool) connect(uri string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), uri)
	redactedUri := utils.GetRedactedURLs(uri)[0]
	if err != nil {
		log.Warnf("Failed to connect to %q: %s", redactedUri, err)
		return nil, err
	}
	log.Infof("Connected to %q", redactedUri)
	err = pool.initSession(conn)
	if err != nil {
		log.Warnf("Failed to set session vars %q: %s", redactedUri, err)
		conn.Close(context.Background())
		conn = nil
	}
	return conn, err
}

func (pool *ConnectionPool) shuffledConnUriList() []string {
	connUriList := make([]string, len(pool.params.ConnUriList))
	copy(connUriList, pool.params.ConnUriList)

	rand.Shuffle(len(connUriList), func(i, j int) {
		connUriList[i], connUriList[j] = connUriList[j], connUriList[i]
	})
	return connUriList
}

func (pool *ConnectionPool) getNextUriIndex() int {
	pool.Lock()
	defer pool.Unlock()

	pool.nextUriIndex = (pool.nextUriIndex + 1) % len(pool.params.ConnUriList)

	return pool.nextUriIndex
}

func (pool *ConnectionPool) initSession(conn *pgx.Conn) error {
	for _, v := range pool.params.SessionInitScript {
		_, err := conn.Exec(context.Background(), v)
		if err != nil {
			if strings.Contains(err.Error(), ERROR_MSG_PERMISSION_DENIED) {
				return nil
			}
			return err
		}
	}
	return nil
}
