//        file: haunt_lock/haunt_master.go
// description: Utility to perform master election/failover using etcd.

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package haunt_lock

import (
	"errors"
	"fmt"
	"path"
	"runtime"
	"sync"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

type EVENT_TYPE int

const (
	MASTER_ADD EVENT_TYPE = iota
	MASTER_DELETE
	MASTER_MODIFY
	MASTER_ERROR
)

const (
	RETRY_SLEEP = 200
)

const (
	HAUNT_MASTER_DIR = "/haunt_master"
)

type MasterEvent struct {
	Type          EVENT_TYPE
	Master        string
	ModifiedIndex uint64
}

type Master interface {
	Start()
	Stop()
	GetEventsChan() <-chan *MasterEvent
	GetMaster() string
	TryAcquire() (ret error)
}

type EtcdLock struct {
	sync.Mutex

	client             *etcd.Client
	name               string
	id                 string
	ttl                uint64
	enable             bool
	master             string
	watchStopChan      chan bool
	eventsChan         chan *MasterEvent
	stoppedChan        chan bool
	refreshStoppedChan chan bool
	ifHolding          bool
	modifiedIndex      uint64
}

func NewMaster(etcdClient *etcd.Client, namespace, name, value string, ttl uint64) Master {
	return &EtcdLock{
		client:             etcdClient,
		name:               path.Join(HAUNT_MASTER_DIR, namespace, name),
		id:                 value,
		ttl:                ttl,
		enable:             false,
		master:             "",
		watchStopChan:      make(chan bool, 1),
		eventsChan:         make(chan *MasterEvent, 1),
		stoppedChan:        make(chan bool, 1),
		refreshStoppedChan: make(chan bool, 1),
		ifHolding:          false,
		modifiedIndex:      0,
	}
}

func (self *EtcdLock) Start() {
	glog.Infof("[EtcdLock][Start] start to acquire lock[%s].", self.name)
	self.Lock()
	if self.enable {
		self.Unlock()
		return
	}
	self.enable = true
	self.Unlock()

	go func() {
		for {
			err := self.acquire()
			if err == nil {
				break
			}
		}
	}()
}

func (self *EtcdLock) Stop() {
	glog.Infof("[EtcdLock][Stop] stop acquire lock[%s].", self.name)
	self.Lock()
	if !self.enable {
		self.Unlock()
		return
	}
	self.enable = false
	self.Unlock()

	self.watchStopChan <- true
	<-self.stoppedChan
}

func (self *EtcdLock) GetEventsChan() <-chan *MasterEvent {
	return self.eventsChan
}

func (self *EtcdLock) GetMaster() string {
	self.Lock()
	defer self.Unlock()
	return self.master
}

func (self *EtcdLock) TryAcquire() (ret error) {
	defer func() {
		if r := recover(); r != nil {
			callers := ""
			for i := 0; true; i++ {
				_, file, line, ok := runtime.Caller(i)
				if !ok {
					break
				}
				callers = callers + fmt.Sprintf("%v:%v\n", file, line)
			}
			errMsg := fmt.Sprintf("[EtcdLock][TryAcquire] Recovered from panic: %#v (%v)\n%v", r, r, callers)
			glog.Errorf(errMsg)
			ret = errors.New(errMsg)
		}
	}()

	rsp, err := self.client.Get(self.name, false, false)
	if err != nil {
		etcdErr, ok := err.(*etcd.EtcdError)
		if ok && etcdErr != nil && etcdErr.ErrorCode == ErrCodeETCDKeyNotFound {
			glog.Infof("[EtcdLock][TryAcquire] try to acquire lock[%s]", self.name)
			rsp, err = self.client.Create(self.name, self.id, self.ttl)
			if err != nil {
				glog.Errorf("[EtcdLock][TryAcquire] etcd create lock[%s] error: %s", self.name, err.Error())
				return err
			}
		} else {
			glog.Errorf("[EtcdLock][TryAcquire] etcd get lock[%s] error: %s", self.name, err.Error())
			return err
		}
	}

	if rsp.Node.Value == self.id {
		glog.V(2).Infof("[EtcdLock][TryAcquire] acquire lock: %s", self.name)
		self.ifHolding = true
		go self.refresh()
	} else {
		return fmt.Errorf("Master[%s] has locked, value[%s]", self.name, rsp.Node.Value)
	}

	return nil
}

func (self *EtcdLock) acquire() (ret error) {
	defer func() {
		if r := recover(); r != nil {
			callers := ""
			for i := 0; true; i++ {
				_, file, line, ok := runtime.Caller(i)
				if !ok {
					break
				}
				callers = callers + fmt.Sprintf("%v:%v\n", file, line)
			}
			errMsg := fmt.Sprintf("[EtcdLock][acquire] Recovered from panic: %#v (%v)\n%v", r, r, callers)
			glog.Errorf(errMsg)
			ret = errors.New(errMsg)
		}
	}()

	var rsp *etcd.Response
	err := fmt.Errorf("Dummy error.")

	for {
		if !self.enable {
			self.stopAcquire()
			break
		}

		if err != nil || rsp.Node.Value == "" {
			rsp, err = self.client.Get(self.name, false, false)
			if err != nil {
				etcdErr, ok := err.(*etcd.EtcdError)
				if ok && etcdErr != nil && etcdErr.ErrorCode == ErrCodeETCDKeyNotFound {
					glog.Infof("[EtcdLock][acquire] try to acquire lock[%s]", self.name)
					rsp, err = self.client.Create(self.name, self.id, self.ttl)
					if err != nil {
						glog.Errorf("[EtcdLock][acquire] etcd create lock[%s] error: %s", self.name, err.Error())
						continue
					}
				} else {
					glog.Errorf("[EtcdLock][acquire] etcd get lock[%s] error: %s", self.name, err.Error())
					time.Sleep(RETRY_SLEEP * time.Millisecond)
					continue
				}
			}
		}

		self.processEtcdRsp(rsp)

		self.Lock()
		self.master = rsp.Node.Value
		self.Unlock()
		self.modifiedIndex = rsp.Node.ModifiedIndex

		var preIdx uint64
		// TODO: maybe change with etcd change
		if rsp.EtcdIndex < rsp.Node.ModifiedIndex {
			preIdx = rsp.Node.ModifiedIndex + 1
		} else {
			preIdx = rsp.EtcdIndex + 1
		}
		rsp, err = self.client.Watch(self.name, preIdx, false, nil, self.watchStopChan)
		if err != nil {
			if etcd.ErrWatchStoppedByUser == err {
				glog.Infof("[EtcdLock][acquire] watch lock[%s] stop by user.", self.name)
			} else {
				glog.Errorf("[EtcdLock][acquire] failed to watch lock[%s] error: %s", self.name, err.Error())
			}
		}
	}

	return nil
}

func (self *EtcdLock) processEtcdRsp(rsp *etcd.Response) {
	if rsp.Node.Value == self.id {
		if !self.ifHolding {
			glog.V(2).Infof("[EtcdLock][processEtcdRsp] acquire lock: %s", self.name)
			self.ifHolding = true
			self.eventsChan <- &MasterEvent{Type: MASTER_ADD, Master: self.id, ModifiedIndex: rsp.Node.ModifiedIndex}
			go self.refresh()
		}
	} else {
		if self.ifHolding {
			glog.Errorf("[EtcdLock][processEtcdRsp] lost lock: %s", self.name)
			self.ifHolding = false
			self.refreshStoppedChan <- true
			self.eventsChan <- &MasterEvent{Type: MASTER_DELETE}
		}
		if self.master != rsp.Node.Value {
			glog.V(2).Infof("[EtcdLock][processEtcdRsp] modify lock[%s] to master[%s]", self.name, rsp.Node.Value)
			self.eventsChan <- &MasterEvent{Type: MASTER_MODIFY, Master: rsp.Node.Value, ModifiedIndex: rsp.Node.ModifiedIndex}
		}
	}
}

func (self *EtcdLock) stopAcquire() {
	if self.ifHolding {
		glog.V(2).Infof("[EtcdLock][stopAcquire] delete lock: %s", self.name)
		_, err := self.client.Delete(self.name, false)
		if err != nil {
			glog.Errorf("[EtcdLock][stopAcquire] failed to delete lock: %s error: %s", self.name, err.Error())
		}
		self.ifHolding = false
		self.refreshStoppedChan <- true
	}
	self.Lock()
	self.master = ""
	self.Unlock()
	self.stoppedChan <- true
}

func (self *EtcdLock) refresh() {
	for {
		select {
		case <-self.refreshStoppedChan:
			glog.V(2).Infof("[EtcdLock][refresh] Stopping refresh for lock %s", self.name)
			return
		case <-time.After(time.Second * time.Duration(self.ttl*4/10)):
			rsp, err := self.client.CompareAndSwap(self.name, self.id, self.ttl, self.id, self.modifiedIndex)
			if err != nil {
				glog.Errorf("[EtcdLock][refresh] Failed to set ttl for lock[%s] error:%s", self.name, err.Error())
			} else {
				self.modifiedIndex = rsp.Node.ModifiedIndex
			}
		}
	}
}
