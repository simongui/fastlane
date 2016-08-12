package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/paulbellamy/ratecounter"
	"github.com/rifflock/lfshook"
	"github.com/robertkrimen/otto"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	host     = "db1.us-east-1.com"
	port     = 33301
	username = "repl_user"
	password = "password"
	vm       *otto.Otto
	script   *otto.Script
	counter  = ratecounter.NewRateCounter(10 * time.Second)
)

// CallInfo Represents caller information.
type CallInfo struct {
	packageName string
	fileName    string
	funcName    string
	line        int
}

func main() {
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			// fmt.Println(counter.Rate())

			logrus.WithFields(logrus.Fields{
				"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
			}).Info(counter.Rate())
		}
	}()

	logrus.SetFormatter(new(prefixed.TextFormatter))

	logrus.StandardLogger().Hooks.Add(lfshook.NewHook(lfshook.PathMap{
		logrus.InfoLevel:  "/var/log/fastlane.log",
		logrus.WarnLevel:  "/var/log/fastlane.log",
		logrus.ErrorLevel: "/var/log/fastlane.log",
		logrus.DebugLevel: "/var/log/fastlane.log",
		logrus.FatalLevel: "/var/log/fastlane.log",
		logrus.PanicLevel: "/var/log/fastlane.log",
	}))

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	}).Info("process started")

	startJavascriptRuntime()
	startReplication()
}

// GetCallInfo Returns the caller information.
func GetCallInfo() *CallInfo {
	pc, file, line, _ := runtime.Caller(1)
	_, fileName := path.Split(file)
	parts := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	pl := len(parts)
	packageName := ""
	funcName := parts[pl-1]

	if parts[pl-2][0] == '(' {
		funcName = parts[pl-2] + "." + funcName
		packageName = strings.Join(parts[0:pl-2], ".")
	} else {
		packageName = strings.Join(parts[0:pl-1], ".")
	}

	return &CallInfo{
		packageName: packageName,
		fileName:    fileName,
		funcName:    funcName,
		line:        line,
	}
}

func startJavascriptRuntime() {
	vm = otto.New()
	script, _ = vm.Compile("example.js", nil)
	vm.Run(script)
}

func startReplication() {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	syncer := replication.NewBinlogSyncer(100, "mysql")

	// Register slave, the MySQL master is at 127.0.0.1:3306, with user root and an empty password
	err := syncer.RegisterSlave(host, uint16(port), username, password)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
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
	streamer, _ := syncer.StartSync(mysql.Position{})

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	}).Infof("MySQL replication link connected to %s:%d", host, port)

	for {
		ev, _ := streamer.GetEvent()
		_, err = vm.Call("handleEvent", nil, ev.Event, strings.ToLower(ev.Header.EventType.String()))
		if err != nil {
			fmt.Println(err)
		}
		// runScript(ev)
		counter.Incr(1)

		// switch e := ev.Event.(type) {
		// case *replication.QueryEvent:
		// 	fmt.Println(string(e.Query))
		// 	// os.Exit(0)
		// case *replication.RotateEvent:
		// 	// fmt.Println(e)
		// }
	}
}

var errhalt = errors.New("Stahp")

func runScript(ev *replication.BinlogEvent) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		if caught := recover(); caught != nil {
			if caught == errhalt {
				fmt.Fprintf(os.Stderr, "Some code took to long! Stopping after: %v\n", duration)
				return
			}
			panic(caught) // Something else happened, repanic!
		}
		// fmt.Fprintf(os.Stderr, "Ran code successfully: %v\n", duration)
	}()

	vm := otto.New()
	vm.Interrupt = make(chan func(), 1) // The buffer prevents blocking

	go func() {
		time.Sleep(5 * time.Second) // Stop after two seconds
		vm.Interrupt <- func() {
			panic(errhalt)
		}
	}()

	go func() {
		s, _ := vm.Compile("example.js", nil)
		vm.Run(s)
		_, err := vm.Call("handleEvent", nil, ev.Event, strings.ToLower(ev.Header.EventType.String()))
		if err != nil {
			fmt.Println(err)
		}
	}()
}
