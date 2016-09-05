package services

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/paulbellamy/ratecounter"
	"github.com/pkg/errors"
	"github.com/robertkrimen/otto"
	"github.com/simongui/fastlane/storage"
)

// ServiceHost ServiceHost represents a service instance.
type ServiceHost struct {
	// store            *storage.BoltDBStore
	store            storage.Store
	jsVirtualMachine *otto.Otto
	jsScript         *otto.Script
	replicator       *MySQLReplicator
	http             *HTTPServer
	redis            *RedisServer
	counter          *ratecounter.RateCounter
}

// ListenAndServe Starts the service.
func (serviceHost *ServiceHost) ListenAndServe(localDatabaseFilename string, store string, httpListenAddress string, redisListenAddress string, mysqlHost string, mysqlPort uint16, mysqlUsername string, mysqlPassword string, mysqlServerID uint32) error {
	serviceHost.counter = ratecounter.NewRateCounter(1 * time.Second)

	serviceHost.startStorage(localDatabaseFilename, store)
	serviceHost.startJavascriptRuntime()

	// Start HTTP protocol server.
	logrus.WithFields(logrus.Fields{
		"listen": httpListenAddress,
	}).Info("starting http server")
	serviceHost.http = NewHTTPServer(serviceHost.store)
	go serviceHost.http.ListenAndServe(httpListenAddress)

	// Start Redis protocol server.
	logrus.WithFields(logrus.Fields{
		"listen": redisListenAddress,
	}).Info("starting redis server")
	serviceHost.redis = NewRedisServer(serviceHost.store)
	go serviceHost.redis.ListenAndServe(redisListenAddress)

	// Start MySQL replication.
	go serviceHost.startReplication(mysqlHost, mysqlPort, mysqlUsername, mysqlPassword, mysqlServerID, serviceHost.store, serviceHost.jsVirtualMachine)

	// Start per second ticker.
	go serviceHost.StartTicker()
	return nil
}

func (serviceHost *ServiceHost) startStorage(filename string, store string) {
	switch store {
	case "boltdb":
		serviceHost.store = &storage.BoltDBStore{}
	case "lmdb":
		serviceHost.store = &storage.LMDBStore{}
	case "rocksdb":
		// serviceHost.store = &storage.RocksDBStore{}
	}

	err := serviceHost.store.Open(filename)
	if err != nil {
		logrus.Fatal(errors.Wrap(err, "unable to open "+filename))
	}
	logrus.Info("opened fastlane.db")
}

func (serviceHost *ServiceHost) startJavascriptRuntime() {
	serviceHost.jsVirtualMachine = otto.New()
	serviceHost.jsScript, _ = serviceHost.jsVirtualMachine.Compile("example.js", nil)
	serviceHost.jsVirtualMachine.Run(serviceHost.jsScript)

	logrus.Info("started javascript runtime")
}

func (serviceHost *ServiceHost) startReplication(host string, port uint16, username string, password string, serverID uint32, store storage.Store, jsVirtualMachine *otto.Otto) {
	serviceHost.replicator = NewMySQLReplicator(serviceHost, host, port, username, password, serverID, store, jsVirtualMachine)
	serviceHost.replicator.startReplication()
}

// IsStarted Returns whether all services have been started.
func (serviceHost *ServiceHost) IsStarted() bool {
	if serviceHost.replicator.IsStarted() == true &&
		serviceHost.store.IsStarted() == true &&
		serviceHost.http.IsStarted() == true &&
		serviceHost.redis.IsStarted() == true {
		return true
	}
	return false
}

// Rate Returns the rate of throughput per second.
func (serviceHost *ServiceHost) Rate() int64 {
	return serviceHost.counter.Rate()
}

// Increment Increments the write operations counter by the specified amount.
func (serviceHost *ServiceHost) Increment(amount int64) {
	serviceHost.counter.Incr(amount)
}

// StartTicker Starts the per second ticker.
func (serviceHost *ServiceHost) StartTicker() {
	ticker := time.NewTicker(time.Second * 1)
	for range ticker.C {
		if serviceHost.IsStarted() {
			logrus.Info(serviceHost.Rate())

			serviceHost.replicator.SaveBinlogPosition()
			err := serviceHost.replicator.store.Commit()
			if err != nil {
				logrus.Error(err)
			}
		}
	}
}
