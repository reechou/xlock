package haunt_lock

import (
	"fmt"
	"testing"
	"time"
)

func goGetSLock(lockName string, sLock *SeizeLock) {
	err := sLock.Lock()
	if err != nil {
		fmt.Println(lockName, err.Error())
		return
	}
	fmt.Println(lockName, "Get Lock success.")
	err = sLock.Lock()
	if err != nil {
		fmt.Println(lockName, err.Error())
		return
	}
	sLock.Unlock()
}

func TestSeizeLock(t *testing.T) {
	sLock1 := NewSeizeLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), "slock", "v1", 10)
	sLock2 := NewSeizeLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), "slock", "v2", 10)
	sLock3 := NewSeizeLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), "slock", "v3", 10)

	go goGetSLock("v1", sLock1)
	go goGetSLock("v2", sLock2)
	go goGetSLock("v3", sLock3)

	sChan := make(chan bool, 1)
	<-sChan
}

func goGetTLock(lockName string, tLock *HauntTimingRWLock) {
	err := tLock.Lock()
	if err != nil {
		fmt.Println(lockName, err.Error())
		return
	}
	fmt.Println(lockName, "get lock. token =", tLock.GetToken())
	time.Sleep(30 * time.Second)
	tLock.Unlock()
}

func TestTimingLock(t *testing.T) {
	tLock1 := NewHauntTimingRWLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), H_LOCK_WRITE, "youzan", "twlock", "v", 30)
	tLock2 := NewHauntTimingRWLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), H_LOCK_WRITE, "youzan", "twlock", "v", 30)
	tLock3 := NewHauntTimingRWLock(NewEtcdClient("192.168.66.205:2379,192.168.66.237:2379"), H_LOCK_READ, "youzan", "twlock", "v", 30)

	go goGetTLock("tLock1", tLock1)
	go goGetTLock("tLock2", tLock2)
	go goGetTLock("tLock3", tLock3)

	sChan := make(chan bool, 1)
	<-sChan
}
