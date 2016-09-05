package services

import (
	"log"
	"strings"
	"sync"

	"github.com/simongui/fastlane/storage"
	"github.com/tidwall/redcon"
)

// RedisServer Represents an instance of a redis protocol server.
type RedisServer struct {
	started bool
	store   storage.Store
}

// NewRedisServer Returns a new RedisServer instance.
func NewRedisServer(store storage.Store) *RedisServer {
	server := &RedisServer{store: store}
	return server
}

// IsStarted Returns whether the Redis protocol server is started.
func (server *RedisServer) IsStarted() bool {
	return server.started
}

// ListenAndServe Starts the Redis protocol server.
func (server *RedisServer) ListenAndServe(address string) {
	var mu sync.RWMutex
	var items = make(map[string]string)

	server.started = true

	err := redcon.ListenAndServe(address,
		func(conn redcon.Conn, commands [][]string) {

			for _, args := range commands {
				switch strings.ToLower(args[0]) {
				default:
					conn.WriteError("ERR unknown command '" + args[0] + "'")
				case "ping":
					conn.WriteString("PONG")
				case "quit":
					conn.WriteString("OK")
					conn.Close()
				case "set":
					if len(args) != 3 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}

					// mu.Lock()
					// items[args[1]] = args[2]
					// mu.Unlock()
					// mu.Lock()
					server.store.Set([]byte(args[1]), []byte(args[2]))
					// mu.Unlock()
					conn.WriteString("OK")
				case "get":
					if len(args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}
					// mu.RLock()
					// val, ok := items[args[1]]
					// mu.RUnlock()
					// go func(con redcon.Conn) {
					val, err := server.store.Get([]byte(args[1]))
					if err != nil {
						conn.WriteNull()
					} else {
						conn.WriteBulk(string(val))
					}
					// }(conn)
				case "del":
					if len(args) != 2 {
						conn.WriteError("ERR wrong number of arguments for '" + args[0] + "' command")
						continue
					}
					mu.Lock()
					_, ok := items[args[1]]
					delete(items, args[1])
					mu.Unlock()
					if !ok {
						conn.WriteInt(0)
					} else {
						conn.WriteInt(1)
					}
				}
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		server.started = false
		log.Fatal(err)
	}
}
