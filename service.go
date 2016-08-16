package main

import (
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/paulbellamy/ratecounter"
	"github.com/robertkrimen/otto"
	"github.com/simongui/fastlane/services"
	"github.com/simongui/fastlane/storage"
)

var (
	vm      *otto.Otto
	script  *otto.Script
	counter = ratecounter.NewRateCounter(1 * time.Second)
)

// Service Service represents a service instance.
type Service struct {
	store      *storage.BoltDBStore
	replicator *MySQLReplicator
	http       *services.HTTPServer
	redis      *services.RedisServer
}

// ListenAndServe Starts the service.
func (service *Service) ListenAndServe(filename string, serverID uint32, httpListenAddress string, redisListenAddress string) error {
	service.startStorage(filename)
	service.startJavascriptRuntime()

	// Start HTTP protocol server.
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		"listen": httpListenAddress,
	}).Info("starting http server")
	service.http = services.NewHTTPServer(service.store)
	go service.http.ListenAndServe(httpListenAddress)

	// Start Redis protocol server.
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		"listen": redisListenAddress,
	}).Info("starting redis server")
	service.redis = services.NewRedisServer(service.store)
	go service.redis.ListenAndServe(redisListenAddress)

	// Start MySQL replication.
	service.startReplication(serverID, service.store)
	return nil
}

func (service *Service) startStorage(filename string) {
	service.store = &storage.BoltDBStore{}
	err := service.store.Open(filename)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		}).Fatal("unable to open fastlane.db")
	}
	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	}).Info("openened fastlane.db")
}

func (service *Service) startJavascriptRuntime() {
	vm = otto.New()
	script, _ = vm.Compile("example.js", nil)
	vm.Run(script)

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	}).Info("started javascript runtime")

}

func (service *Service) startReplication(serverID uint32, store *storage.BoltDBStore) {
	service.replicator = NewMySQLReplicator(serverID, store)
	service.replicator.startReplication()
}
