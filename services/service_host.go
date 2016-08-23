package services

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/paulbellamy/ratecounter"
	"github.com/robertkrimen/otto"
	"github.com/simongui/fastlane/common"
	"github.com/simongui/fastlane/storage"
)

// ServiceHost ServiceHost represents a service instance.
type ServiceHost struct {
	store            *storage.BoltDBStore
	jsVirtualMachine *otto.Otto
	jsScript         *otto.Script
	replicator       *MySQLReplicator
	http             *HTTPServer
	redis            *RedisServer
	counter          *ratecounter.RateCounter
}

// ListenAndServe Starts the service.
func (serviceHost *ServiceHost) ListenAndServe(localDatabaseFilename string, httpListenAddress string, redisListenAddress string, mysqlHost string, mysqlPort uint16, mysqlUsername string, mysqlPassword string, mysqlServerID uint32) error {
	serviceHost.counter = ratecounter.NewRateCounter(1 * time.Second)

	serviceHost.startStorage(localDatabaseFilename)
	serviceHost.startJavascriptRuntime()

	// Start HTTP protocol server.
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
		"listen": httpListenAddress,
	}).Info("starting http server")
	serviceHost.http = NewHTTPServer(serviceHost.store)
	go serviceHost.http.ListenAndServe(httpListenAddress)

	// Start Redis protocol server.
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
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

func (serviceHost *ServiceHost) startStorage(filename string) {
	serviceHost.store = &storage.BoltDBStore{}
	err := serviceHost.store.Open(filename)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
		}).Fatal("unable to open fastlane.db")
	}
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
	}).Info("opened fastlane.db")
}

func (serviceHost *ServiceHost) startJavascriptRuntime() {
	serviceHost.jsVirtualMachine = otto.New()
	serviceHost.jsScript, _ = serviceHost.jsVirtualMachine.Compile("example.js", nil)
	serviceHost.jsVirtualMachine.Run(serviceHost.jsScript)

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
	}).Info("started javascript runtime")

}

func (serviceHost *ServiceHost) startReplication(host string, port uint16, username string, password string, serverID uint32, store *storage.BoltDBStore, jsVirtualMachine *otto.Otto) {
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
			logrus.WithFields(logrus.Fields{
				"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
			}).Info(serviceHost.Rate())

			serviceHost.replicator.SaveBinlogPosition()
			serviceHost.replicator.store.Commit()
		}
	}
}
