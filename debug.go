package haunt_lock

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

const (
	LOG_DEFAULT = iota
	LOG_ERROR
	LOG_INFO
	LOG_DEBUG
)

var logger *lockLogger

func SetLogger(l *log.Logger, level int) {
	logger = &lockLogger{log: l, level: level}
	if level >= LOG_DEBUG {
		etcd.SetLogger(l)
	}
}

type lockLogger struct {
	log   *log.Logger
	level int
}

func (p *lockLogger) Info(args ...interface{}) {
	if p.level >= LOG_INFO {
		msg := "INFO: " + fmt.Sprint(args...)
		p.log.Println(msg)
	}
}

func (p *lockLogger) Infof(f string, args ...interface{}) {
	if p.level >= LOG_INFO {
		msg := "INFO: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Print(msg)
	}
}

func (p *lockLogger) Error(args ...interface{}) {
	if p.level >= LOG_INFO {
		msg := "ERROR: " + fmt.Sprint(args...)
		p.log.Println(msg)
	}
}

func (p *lockLogger) Errorf(f string, args ...interface{}) {
	if p.level >= LOG_INFO {
		msg := "ERROR: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Print(msg)
	}
}

func init() {
	// Default logger uses the go default log.
	SetLogger(log.New(ioutil.Discard, "go-x-lock", log.LstdFlags), LOG_DEBUG)
}
