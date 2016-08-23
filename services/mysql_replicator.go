package services

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/robertkrimen/otto"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/simongui/fastlane/common"
	"github.com/simongui/fastlane/storage"
)

// MySQLReplicator Represents an instance of a MySQL replication client.
type MySQLReplicator struct {
	serviceHost      *ServiceHost
	started          bool
	host             string
	port             uint16
	username         string
	password         string
	serverID         uint32
	store            *storage.BoltDBStore
	jsVirtualMachine *otto.Otto
	mysqlPosition    mysql.Position
}

// NewMySQLReplicator Returns a new instance of NewMySQLReplicator.
func NewMySQLReplicator(serviceHost *ServiceHost, host string, port uint16, username string, password string, serverID uint32, store *storage.BoltDBStore, jsVirtualMachine *otto.Otto) *MySQLReplicator {
	replicator := &MySQLReplicator{
		serviceHost:      serviceHost,
		host:             host,
		port:             port,
		username:         username,
		password:         password,
		serverID:         serverID,
		store:            store,
		jsVirtualMachine: jsVirtualMachine,
		mysqlPosition:    mysql.Position{},
	}
	return replicator
}

// IsStarted Returns whether the MySQL replication service is started.
func (replicator *MySQLReplicator) IsStarted() bool {
	return replicator.started
}

func (replicator *MySQLReplicator) startReplication() {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	syncer := replication.NewBinlogSyncer(replicator.serverID, "mysql")

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err := syncer.RegisterSlave(replicator.host, replicator.port, replicator.username, replicator.password)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
		}).Panic("can't start binlog" + err.Error())
	}

	// server := failover.NewServer(host+":"+strconv.Itoa(port), failover.User{Name: "root", Password: password}, failover.User{Name: username, Password: password})
	//
	// results, err := server.MasterStatus()
	// if err != nil {
	// 	logrus.WithFields(logrus.Fields{
	// 		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	// 	}).Panic("getting master status failed" + err.Error())
	// }
	// binlogFile, err := results.GetStringByName(0, "File")
	// if err != nil {
	// 	logrus.WithFields(logrus.Fields{
	// 		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	// 	}).Panic("getting binlog file failed" + err.Error())
	// }
	// binlogPos, err := results.GetUintByName(0, "Position")
	// if err != nil {
	// 	logrus.WithFields(logrus.Fields{
	// 		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	// 	}).Panic("getting binlog position failed" + err.Error())
	// }

	// Start sync with sepcified binlog file and position
	// streamer, _ := syncer.StartSync(mysql.Position{binlogFile, uint32(binlogPos)})

	binlogInfo, err := replicator.store.GetBinlogPosition()
	if err == nil {
		replicator.mysqlPosition.Name = binlogInfo.File
		replicator.mysqlPosition.Pos = binlogInfo.Position
	}

	streamer, _ := syncer.StartSync(replicator.mysqlPosition)

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	logrus.WithFields(logrus.Fields{
		"prefix":   fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
		"host":     replicator.host,
		"port":     replicator.port,
		"binlog":   replicator.mysqlPosition.Name,
		"position": replicator.mysqlPosition.Pos,
	}).Infof("started mysql replication link")

	for {
		ev, _ := streamer.GetEvent()
		if ev == nil {
			logrus.WithFields(logrus.Fields{
				"prefix":   fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
				"host":     replicator.host,
				"port":     replicator.port,
				"binlog":   replicator.mysqlPosition.Name,
				"position": replicator.mysqlPosition.Pos,
			}).Fatalf("unable to stream from binlog")
		}
		_, err = replicator.jsVirtualMachine.Call("handleEvent", nil, ev.Event, strings.ToLower(ev.Header.EventType.String()))
		if err != nil {
			fmt.Println(err)
		}
		// runScript(ev)

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			replicator.mysqlPosition.Name = string(e.NextLogName)
			replicator.mysqlPosition.Pos = uint32(e.Position)

			logrus.WithFields(logrus.Fields{
				"prefix":   fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
				"file":     replicator.mysqlPosition.Name,
				"position": replicator.mysqlPosition.Pos,
			}).Infof("mysql binlog rotated")
		default:
			replicator.started = true
			replicator.mysqlPosition.Pos = ev.Header.LogPos
		}
		replicator.serviceHost.Increment(1)
	}
}

// SaveBinlogPosition Saves the current MySQL binlog filename and position.
func (replicator *MySQLReplicator) SaveBinlogPosition() {
	file := replicator.mysqlPosition.Name
	position := replicator.mysqlPosition.Pos
	err := replicator.store.SetBinlogPosition(&storage.BinlogInformation{File: file, Position: position})

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix":   fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
			"file":     replicator.mysqlPosition.Name,
			"position": replicator.mysqlPosition.Pos,
		}).Infof("failed to save mysql binlog position")
	} else {
		logrus.WithFields(logrus.Fields{
			"prefix":   fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
			"file":     replicator.mysqlPosition.Name,
			"position": replicator.mysqlPosition.Pos,
		}).Infof("saved mysql binlog position")
	}
}

// var errhalt = errors.New("Stahp")
//
// func runScript(ev *replication.BinlogEvent) {
// 	start := time.Now()
// 	defer func() {
// 		duration := time.Since(start)
// 		if caught := recover(); caught != nil {
// 			if caught == errhalt {
// 				fmt.Fprintf(os.Stderr, "Some code took to long! Stopping after: %v\n", duration)
// 				return
// 			}
// 			panic(caught) // Something else happened, repanic!
// 		}
// 		// fmt.Fprintf(os.Stderr, "Ran code successfully: %v\n", duration)
// 	}()
//
// 	vm := otto.New()
// 	vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking
//
// 	go func() {
// 		time.Sleep(5 * time.Second) // Stop after two seconds
// 		vm.Interrupt <- func() {
// 			panic(errhalt)
// 		}
// 	}()
//
// 	go func() {
// 		s, _ := vm.Compile("example.js", nil)
// 		vm.Run(s)
// 		_, err := vm.Call("handleEvent", nil, ev.Event, strings.ToLower(ev.Header.EventType.String()))
// 		if err != nil {
// 			fmt.Println(err)
// 		}
// 	}()
// }
