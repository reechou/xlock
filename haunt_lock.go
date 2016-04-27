//        file: haunt_lock/haunt_lock.go
// description: Distributed read/write lock implementation using etcd.

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

// 锁服务可以分为两类：①保持独占 ②控制时序
// 保持独占：所有试图来获取这个锁的客户端，最终只有一个可以成功获得这把锁
// 控制时序：所有试图来获取这个锁的客户端，最终都是会被安排执行，只是有个全局时序
package haunt_lock

import (
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"runtime"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
)

type LOCK_TYPE int
type TOKEN string

var (
	// about huant seize lock err
	ErrGetSeizeLock  = errors.New("Lock is exist.")
	ErrSeizeLockAg   = errors.New("Lock same value again.")
	// about haunt timing lock err
	ErrLockMarshal   = errors.New("Marshal error.")
	ErrLockUnmarshal = errors.New("Unmarshal error.")
	ErrEnqueueLock   = errors.New("Enqueue lock queue error.")
	ErrLockExpired   = errors.New("Lock expired.")
	ErrLockReqLost   = errors.New("Lock request lost")
	ErrLockGet       = errors.New("Error reading lock queue.")
	ErrLockDelete    = errors.New("Error deleting lock.")
)

const (
	HAUNT_SEIZE_LOCK_DIR  = "/haunt_seize_lock"
	HAUNT_TIMING_LOCK_DIR = "/haunt_timing_lock"
)

const (
	H_LOCK_READ LOCK_TYPE = iota
	H_LOCK_WRITE
)

type LockValue struct {
	LockType string
	ID       string
}

// 分布式抢占锁 Distributed seize lock
// 把etcd上的一个node看作是一把锁，通过create node的方式来实现，若acquire成功，会refresh锁，停止请unlock()
type SeizeLock struct {
	client    *etcd.Client
	name      string
	value     string
	ttl       uint64
	ifHolding bool

	refreshStopCh chan bool
	modifiedIndex uint64
}

func NewSeizeLock(etcdClient *etcd.Client, name, value string, ttl uint64) *SeizeLock {
	return &SeizeLock{
		client:        etcdClient,
		name:          name,
		value:         value,
		ttl:           ttl,
		ifHolding:     false,
		refreshStopCh: make(chan bool, 1),
		modifiedIndex: 0,
	}
}

func (self *SeizeLock) Lock() (ret error) {
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
		if IfETCDKeyNotFound(err) {
			glog.Infof("[SeizeLock][Lock] try to acquire lock[%s]", self.name)
			rsp, err = self.client.Create(self.name, self.value, self.ttl)
			if err != nil {
				glog.Errorf("[SeizeLock][Lock] etcd create lock[%s] error: %s", self.name, err.Error())
				return err
			}
			if rsp.Node.Value == self.value {
				glog.Infof("[SeizeLock] acquire lock[%s]", self.name)
				self.ifHolding = true
				self.modifiedIndex = rsp.Node.ModifiedIndex
				go self.refresh()
			}
			return nil
		} else {
			glog.Errorf("[SeizeLock][Lock] etcd get lock[%s] error: %s", self.name, err.Error())
			return err
		}
	}
	if rsp.Node.Value == self.value {
		glog.Infof("[SeizeLock][Lock] get lock[%s] has exist with you[%s].", self.name, self.value)
		return ErrSeizeLockAg
	}
	glog.Infof("[SeizeLock][Lock] get lock[%s] failed, lock exist value[%s]", self.name, rsp.Node.Value)

	return ErrGetSeizeLock
}

func (self *SeizeLock) Unlock() error {
	if self.ifHolding {
		_, err := self.client.CompareAndDelete(self.name, self.value, 0)
		if err != nil {
			glog.Errorf("[SeizeLock][Unlock] delete lock[%s] error: %s", self.name, err.Error())
		}
		self.ifHolding = false
		self.refreshStopCh <- true
	}

	return nil
}

func (self *SeizeLock) refresh() {
	for {
		select {
		case <-self.refreshStopCh:
			glog.V(2).Infof("Stopping seize lock[%s] refresh.", self.name)
			return
		case <-time.After(time.Second * time.Duration(self.ttl*4/10)):
			if rsp, err := self.client.CompareAndSwap(self.name, self.value, self.ttl, self.value, self.modifiedIndex); err != nil {
				etcdErr, ok := err.(*etcd.EtcdError)
				if ok {
					if etcdErr.ErrorCode != etcd.ErrCodeEtcdNotReachable {
						// if ! not reachable, maybe value changed, return.
						glog.V(2).Infof("Changed seize lock[%s] to value[]%s.", self.name, rsp.Node.Value)
						return
					}
				} else {
					// stop refresh
					return
				}
			} else {
				self.modifiedIndex = rsp.Node.ModifiedIndex
			}
		}
	}
}

// 分布式时序锁 Distributed timing lock
// ETCD维持一份sequence，保证子节点创建的时序性，从而也形成了每个客户端的全局时序
type HauntTimingRWLock struct {
	client     *etcd.Client
	name       string
	id         string
	ttl        uint64
	queueTTL   uint64
	token      string
	refreshKey string
	lockType   LOCK_TYPE
	stop       chan bool
}

var LockTypes = map[LOCK_TYPE]string{H_LOCK_READ: "haunt-read-lock", H_LOCK_WRITE: "haunt-write-lock"}

func NewHauntTimingRWLock(etcdClient *etcd.Client, lockType LOCK_TYPE, namespace, name, value string, ttl uint64) *HauntTimingRWLock {
	return &HauntTimingRWLock{
		client:   etcdClient,
		name:     path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name),
		id:       value,
		ttl:      ttl,
		queueTTL: 30,
		token:    "",
		lockType: lockType,
		stop:     make(chan bool, 1),
	}
}

func ParseTimingLockValue(value string) (string, error) {
	var lockValue LockValue
	err := json.Unmarshal([]byte(value), &lockValue)
	if err != nil {
		return "", err
	}
	return lockValue.ID, nil
}

func (self *HauntTimingRWLock) Lock() error {
	glog.Infof("[HauntTimingRWLock][lock] lock[%s-%s] ttl[%d]", self.name, LockTypes[self.lockType], self.ttl)

//	self.id = getID()
	err := self.enqueueLock(self.id)
	if err != nil {
		return err
	}

	if err := self.waitLock(); err != nil {
		return err
	}

	return nil
}

func (self *HauntTimingRWLock) Unlock() error {
	glog.Infof("[HauntTimingRWLock][unlock] unlock lock[%s] token[%s]", self.name, self.token)

	if _, err := self.client.Delete(self.refreshKey, false); err != nil {
		glog.Errorf("[HauntTimingRWLock][unlock] failed to delete key[%s] error: %s", self.refreshKey, err.Error())
		return ErrLockDelete
	}

	return nil
}

func (self *HauntTimingRWLock) UnlockByToken(token string) error {
	self.refreshKey = path.Join(self.name, token)
	return self.Unlock()
}

func (self *HauntTimingRWLock) GetToken() string {
	return self.token
}

func (self *HauntTimingRWLock) StopLock() {
	close(self.stop)
}

func (self *HauntTimingRWLock) enqueueLock(id string) error {
	value := &LockValue{
		LockType: LockTypes[self.lockType],
		ID:       id,
	}
	valueJson, err := json.Marshal(value)
	if err != nil {
		glog.Errorf("[HauntTimingRWLock][enqueueLock] failed to Marshal name[%s] id[%s] error: %s", self.name, id, err.Error())
		return ErrLockMarshal
	}

	rsp, err := self.client.CreateInOrder(self.name, string(valueJson), self.queueTTL)
	if err != nil {
		glog.Errorf("[HauntTimingRWLock][enqueueLock] failed to CreateInOrder name[%s] id[%s] error:%s", self.name, id, err.Error())
		return ErrEnqueueLock
	}

	_, token := path.Split(rsp.Node.Key)
	self.token = token
	self.refreshKey = path.Join(self.name, token)
	glog.V(2).Infof("[HauntTimingRWLock][enqueueLock] Got token[%s] for lock[%s] with id[%s]", token, self.name, id)

	return nil
}

func (self *HauntTimingRWLock) waitLock() error {
	watchChan := make(chan *etcd.Response, 1)
	watchFailChan := make(chan bool, 1)
	watchStopChan := make(chan bool, 1)

	go func() {
		glog.Infof("[HauntTimingRWLock][waitLock] start to watch lock[%s-%s] token[%s]", self.name, LockTypes[self.lockType], self.token)
		self.watch(watchChan, watchStopChan, watchFailChan)
	}()

	defer func() {
		glog.Infof("[HauntTimingRWLock][waitLock] stop to watch lock[%s-%s] token[%s]", self.name, LockTypes[self.lockType], self.token)
		watchStopChan <- true
	}()

	if ifGot, err := self.tryAcquireLock(); err != nil {
		return err
	} else if ifGot {
		glog.Infof("[HauntTimingRWLock][waitLock] got the lock[%s-%s] token[%s]", self.name, LockTypes[self.lockType], self.token)
		return nil
	}

	for {
		select {
		case rsp := <-watchChan:
			if rsp == nil {
				glog.Info("[HauntTimingRWLock][waitLock] got nil rsp in watch channel.")
				continue
			}
			glog.Infof("[HauntTimingRWLock][waitLock] watch rsp action[%s] lock[%s-%s] token[%s]", rsp.Action, self.name, LockTypes[self.lockType], self.token)
			if rsp.Action == "expire" || rsp.Action == "delete" {
				if _, t := path.Split(rsp.Node.Key); t == self.token {
					glog.Errorf("[HauntTimingRWLock][waitLock] lack[%s] token[%s] expired", self.name, self.token)
					return ErrLockExpired
				}
				if ifGot, err := self.tryAcquireLock(); err != nil {
					return err
				} else if ifGot {
					glog.Infof("[HauntTimingRWLock][waitLock] got the lock[%s-%s] token[%s]", self.name, LockTypes[self.lockType], self.token)
					return nil
				}
			}
		case <-watchFailChan:
			watchChan = make(chan *etcd.Response, 1)
			go self.watch(watchChan, watchStopChan, watchFailChan)
		case <-self.stop:
			glog.Infof("[HauntTimingRWLock][waitLock] stop lock.")
			return fmt.Errorf("Stop lock by user.")
		}
	}

	return nil
}

func (self *HauntTimingRWLock) tryAcquireLock() (bool, error) {
	rsp, err := self.client.Get(self.name, true, true)
	if err != nil {
		glog.Errorf("[HauntTimingRWLock][tryAcquireLock] etcd get lock[%s] error: %s", self.name, err.Error())
		return false, ErrLockGet
	}
	for i, node := range rsp.Node.Nodes {
		var value LockValue
		if err := json.Unmarshal([]byte(node.Value), &value); err != nil {
			glog.Errorf("[HauntTimingRWLock][tryAcquireLock] json unmarshal error: %s", err.Error())
			return false, ErrLockUnmarshal
		}
		_, t := path.Split(node.Key)
		if self.lockType == H_LOCK_WRITE && i == 0 {
			if value.LockType != LockTypes[H_LOCK_WRITE] || t != self.token {
				return false, nil
			}
			if err := self.refreshTTL(); err != nil {
				return false, err
			}
			return true, nil
		}
		if value.LockType != LockTypes[H_LOCK_READ] {
			return false, nil
		}
		if t == self.token {
			if err := self.refreshTTL(); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, ErrLockReqLost
}

func (self *HauntTimingRWLock) refreshTTL() error {
	value := &LockValue{
		LockType: LockTypes[self.lockType],
		ID:       self.id,
	}
	valueBytes, err := json.Marshal(value)
	if err != nil {
		glog.Errorf("[HauntTimingRWLock][refreshTTL] failed to marshal value lock[%s] error: %s", self.name, err.Error())
		return ErrLockMarshal
	}
	_, err = self.client.Update(self.refreshKey, string(valueBytes), self.ttl)
	if err != nil {
		glog.Errorf("[HauntTimingRWLock][refreshTTL] failed to refresh lock[%s] error: %s", self.name, err.Error())
		return ErrLockExpired
	}
	return nil
}

func (self *HauntTimingRWLock) watch(watchCh chan *etcd.Response, watchStopCh chan bool, watchFailCh chan bool) {
	_, err := self.client.Watch(self.name, 0, true, watchCh, watchStopCh)
	if err == etcd.ErrWatchStoppedByUser {
		return
	} else {
		glog.Errorf("[HauntTimingRWLock][watch] watch key[%s] error: %s", self.name, err.Error())
		watchFailCh <- true
	}
}
