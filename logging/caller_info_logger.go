package logging

import (
	"path"
	"runtime"
	"strings"

	"github.com/Sirupsen/logrus"
)

// ContextHook Hook to intercept Logrus entries..
type ContextHook struct{}

// Levels Logging Levels.
func (hook ContextHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire Handles Logrus entry hook.
func (hook ContextHook) Fire(entry *logrus.Entry) error {
	pc := make([]uintptr, 3, 3)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !strings.Contains(name, "github.com/Sirupsen/logrus") {
			file, line := fu.FileLine(pc[i] - 1)
			// caller := fmt.Sprintf("%s:%s:%d: %s", path.Base(file), path.Base(name), line, entry.Message)
			// caller := fmt.Sprintf("%s:%d: %s", path.Base(file), line, entry.Message)
			// entry.Message = caller
			entry.Data["file"] = path.Base(file)
			entry.Data["func"] = path.Base(name)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}
