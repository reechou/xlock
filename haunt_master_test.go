package haunt_lock

import (
	"fmt"
	"testing"
	"time"
)

func getMaster(host string, master *EtcdLock) {
	for {
		time.Sleep(10 * time.Second)
		fmt.Println(host, master.GetMaster())
		if host == "127.0.0.1" {
			master.Stop()
			return
		}
	}
}

func goEventLoop(host string, eventChan <-chan *MasterEvent) {
	for {
		select {
		case e := <-eventChan:
			if e.Type == MASTER_ADD {
				// Acquired the lock.
				fmt.Println(host, "EVENT: ADD", e.Master, e.ModifiedIndex)
			} else if e.Type == MASTER_DELETE {
				// Lost the lock.
				fmt.Println(host, "EVENT: DELETE")
			} else {
				// Lock ownership changed.
				fmt.Println(host, "EVENT MODIFY", e.Master, e.ModifiedIndex)
			}
		}
	}
}

func goMaster(host string, master *EtcdLock) {
	fmt.Println(host, "master lock start.")
	go goEventLoop(host, master.GetEventsChan())
	master.Start()
}

func TestMaster(t *testing.T) {
	etcdClient := NewEClient("http://etcd-dev.s.qima-inc.com:2379")
	lock := NewMaster(etcdClient, "mtest", "127.0.0.1", 30)
	lock2 := NewMaster(etcdClient, "mtest", "127.0.0.2", 30)
	lock3 := NewMaster(etcdClient, "mtest", "127.0.0.3", 30)

	go goMaster("127.0.0.1", lock.(*EtcdLock))
	go goMaster("127.0.0.2", lock2.(*EtcdLock))
	go goMaster("127.0.0.3", lock3.(*EtcdLock))

	go getMaster("127.0.0.1", lock.(*EtcdLock))
	go getMaster("127.0.0.2", lock2.(*EtcdLock))
	go getMaster("127.0.0.3", lock3.(*EtcdLock))

	stopChan := make(chan bool, 1)
	<-stopChan
}
