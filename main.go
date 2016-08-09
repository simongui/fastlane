package main

import (
	"fmt"
	"path"
	"runtime"
	"strings"

	"github.com/2tvenom/myreplication"
	"github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	host     = "db1.us-east-1.com"
	port     = 33301
	username = "repl_user"
	password = "password"
)

// CallInfo Represents caller information.
type CallInfo struct {
	packageName string
	fileName    string
	funcName    string
	line        int
}

func main() {
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

	newConnection := myreplication.NewConnection()
	serverID := uint32(2)
	err := newConnection.ConnectAndAuth(host, port, username, password)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		}).Panic("can't connect to MySQL master" + err.Error())
	}

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
	}).Infof("MySQL replication link connected to %s:%d", host, port)

	//Get position and file name
	pos, filename, err := newConnection.GetMasterStatus()

	if err != nil {
		panic("Master status fail: " + err.Error())
	}

	el, err := newConnection.StartBinlogDump(pos, filename, serverID)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		}).Panic("can't start binlog" + err.Error())
	}
	events := el.GetEventChan()
	go func() {
		for {
			event := <-events

			switch e := event.(type) {
			case *myreplication.QueryEvent:
				//Output query event
				println(e.GetQuery())
			case *myreplication.IntVarEvent:
				//Output last insert_id  if statement based replication
				println(e.GetValue())
			case *myreplication.WriteEvent:
				//Output Write (insert) event
				println("Write", e.GetTable())
				//Rows loop
				for i, row := range e.GetRows() {
					//Columns loop
					for j, col := range row {
						//Output row number, column number, column type and column value
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *myreplication.DeleteEvent:
				//Output delete event
				println("Delete", e.GetTable())
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			case *myreplication.UpdateEvent:
				//Output update event
				println("Update", e.GetTable())
				//Output old data before update
				for i, row := range e.GetRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
				//Output new
				for i, row := range e.GetNewRows() {
					for j, col := range row {
						println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
					}
				}
			default:
			}
		}
	}()
	err = el.Start()
	println(err.Error())
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
