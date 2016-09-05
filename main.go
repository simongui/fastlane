package main

import (
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Sirupsen/logrus"
	"github.com/rifflock/lfshook"
	"github.com/simongui/fastlane/logging"
	"github.com/simongui/fastlane/services"
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
	logpath            = kingpin.Flag("logpath", "Where to store the log files").String()
	httpListenAddress  = kingpin.Flag("http-address", "Address for the HTTP protocol server to listen on.").Default(":8000").String()
	redisListenAddress = kingpin.Flag("redis-address", "Address for the Redis protocol server to listen on.").Default(":6379").String()
	store              = kingpin.Flag("store", "Type of storage to use (boltdb or lmdb)").Default("boltdb").String()
	dbpath             = kingpin.Flag("dbpath", "Where to store the database files").Default("fastlane.db").String()
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

	if *verbose {
		logrus.AddHook(logging.ContextHook{})
	}

	// Write log entries to a file on disk if enabled.
	if *logpath != "" {
		logrus.StandardLogger().Hooks.Add(lfshook.NewHook(lfshook.PathMap{
			logrus.InfoLevel:  *logpath,
			logrus.WarnLevel:  *logpath,
			logrus.ErrorLevel: *logpath,
			logrus.DebugLevel: *logpath,
			logrus.FatalLevel: *logpath,
			logrus.PanicLevel: *logpath,
		}))
	}

	logrus.Info("process started")

	service = &services.ServiceHost{}
	go service.ListenAndServe(*dbpath, *store, *httpListenAddress, *redisListenAddress, mysqlHost, mysqlPort, mysqlUsername, mysqlPassword, 9999)

	waitGroup.Add(1)
	waitGroup.Wait()
}
