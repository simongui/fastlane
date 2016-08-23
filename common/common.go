package common

import (
	"path"
	"runtime"
	"strings"
)

// CallInfo Represents caller information.
type CallInfo struct {
	PackageName string
	FileName    string
	FuncName    string
	Line        int
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
		PackageName: packageName,
		FileName:    fileName,
		FuncName:    funcName,
		Line:        line,
	}
}
