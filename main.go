package main

import (
	"fmt"
	"path"
	"runtime"
	"strings"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	host     = "db1.us-east-1.com"
	port     = 33301
	username = "repl_user"
	password = "password"
	service  *Service
)

// CallInfo Represents caller information.
type CallInfo struct {
	packageName string
	fileName    string
	funcName    string
	line        int
}

var (
	verbose            = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
	httpListenAddress  = kingpin.Flag("http-address", "Address for the HTTP protocol server to listen on.").Default(":8000").String()
	redisListenAddress = kingpin.Flag("redis-address", "Address for the Redis protocol server to listen on.").Default(":6379").String()
)

func main() {

	// f, err := os.Create("profile.prof")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	//
	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// go func() {
	// 	for range c {
	// 		pprof.StopCPUProfile()
	// 	}
	// }()

	kingpin.Parse()

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

	go func() {
		startTicker()
	}()

	service = &Service{}
	service.ListenAndServe("fastlane.db", 9999, *httpListenAddress, *redisListenAddress)
}

func startTicker() {
	ticker := time.NewTicker(time.Second * 1)
	for range ticker.C {
		logrus.WithFields(logrus.Fields{
			"prefix": fmt.Sprintf("%s.%s:%d", GetCallInfo().packageName, GetCallInfo().funcName, GetCallInfo().line),
		}).Info(counter.Rate())

		service.replicator.SaveBinlogPosition()
	}
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
