
package haunt_lock

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"

	"github.com/coreos/go-etcd/etcd"
)

var logger *lockLogger

func SetLogger(l *log.Logger) {
	logger = &lockLogger{l}
	etcd.SetLogger(l)
}

type lockLogger struct {
	log *log.Logger
}

func (p *lockLogger) Info(args ...interface{}) {
	msg := "INFO: " + fmt.Sprint(args...)
	p.log.Println(msg)
}

func (p *lockLogger) Infof(f string, args ...interface{}) {
	msg := "INFO: " + fmt.Sprintf(f, args...)
	// Append newline if necessary
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	p.log.Print(msg)
}

func (p *lockLogger) Warning(args ...interface{}) {
	msg := "WARNING: " + fmt.Sprint(args...)
	p.log.Println(msg)
}

func (p *lockLogger) Warningf(f string, args ...interface{}) {
	msg := "WARNING: " + fmt.Sprintf(f, args...)
	// Append newline if necessary
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	p.log.Print(msg)
}

func (p *lockLogger) Error(args ...interface{}) {
	msg := "ERROR: " + fmt.Sprint(args...)
	p.log.Println(msg)
}

func (p *lockLogger) Errorf(f string, args ...interface{}) {
	msg := "ERROR: " + fmt.Sprintf(f, args...)
	// Append newline if necessary
	if !strings.HasSuffix(msg, "\n") {
		msg = msg + "\n"
	}
	p.log.Print(msg)
}

func init() {
	// Default logger uses the go default log.
	SetLogger(log.New(ioutil.Discard, "go-x-lock", log.LstdFlags))
}

