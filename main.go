package main

import (
	"fmt"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/simongui/fastlane/common"
	"github.com/simongui/fastlane/services"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

var (
	mysqlHost     = "db1.us-east-1.com"
	mysqlPort     = uint16(33301)
	mysqlUsername = "repl_user"
	mysqlPassword = "password"
	service       *services.ServiceHost
	waitGroup     sync.WaitGroup
)

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

	// logrus.StandardLogger().Hooks.Add(lfshook.NewHook(lfshook.PathMap{
	// 	logrus.InfoLevel:  "/var/log/fastlane.log",
	// 	logrus.WarnLevel:  "/var/log/fastlane.log",
	// 	logrus.ErrorLevel: "/var/log/fastlane.log",
	// 	logrus.DebugLevel: "/var/log/fastlane.log",
	// 	logrus.FatalLevel: "/var/log/fastlane.log",
	// 	logrus.PanicLevel: "/var/log/fastlane.log",
	// }))

	logrus.WithFields(logrus.Fields{
		"prefix": fmt.Sprintf("%s.%s:%d", common.GetCallInfo().PackageName, common.GetCallInfo().FuncName, common.GetCallInfo().Line),
	}).Info("process started")

	service = &services.ServiceHost{}
	go service.ListenAndServe("fastlane.db", *httpListenAddress, *redisListenAddress, mysqlHost, mysqlPort, mysqlUsername, mysqlPassword, 9999)

	waitGroup.Add(1)
	waitGroup.Wait()
}
