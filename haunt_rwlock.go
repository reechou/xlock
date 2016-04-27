package haunt_lock

import (
	"encoding/json"
	//	"fmt"
	"path"

	"github.com/coreos/go-etcd/etcd"
)

func RLock(client *etcd.Client, namespace, name, value string, ttl uint64) (TOKEN, error) {
	return rwLock(client, H_LOCK_READ, namespace, name, value, ttl)
}

func WLock(client *etcd.Client, namespace, name, value string, ttl uint64) (TOKEN, error) {
	return rwLock(client, H_LOCK_WRITE, namespace, name, value, ttl)
}

func RUnlock(client *etcd.Client, namespace, name string, token TOKEN) error {
	return rwUnlock(client, namespace, name, token)
}

func WUnlock(client *etcd.Client, namespace, name string, token TOKEN) error {
	return rwUnlock(client, namespace, name, token)
}

func rwLock(client *etcd.Client, lockType LOCK_TYPE, namespace, name, value string, ttl uint64) (TOKEN, error) {
	logger.Infof("[rwLock] lock[%s-%s] ttl[%d]", name, LockTypes[lockType], ttl)

	token, err := enqueueLock(client, lockType, namespace, name, value, ttl)
	if err != nil {
		return "", err
	}

	if err := waitForLock(client, lockType, namespace, name, value, token, ttl); err != nil {
		return "", err
	}

	return TOKEN(token), nil
}

func rwUnlock(client *etcd.Client, namespace, name string, token TOKEN) error {
	logger.Infof("[rwUnlock] unlock lock[%s] token[%s]", name, token)

	if _, err := client.Delete(path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name, string(token)), false); err != nil {
		logger.Errorf("[HauntTimingRWLock][unlock] failed to delete key[%s] error: %s", path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name, string(token)), err.Error())
		return ErrLockDelete
	}

	return nil
}

func enqueueLock(client *etcd.Client, lockType LOCK_TYPE, namespace, name, value string, ttl uint64) (string, error) {
	valueL := &LockValue{
		LockType: LockTypes[lockType],
		ID:       value,
	}
	valueJson, err := json.Marshal(valueL)
	if err != nil {
		logger.Errorf("[enqueueLock] failed to Marshal name[%s] id[%s] error: %s", name, value, err.Error())
		return "", ErrLockMarshal
	}

	key := path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name)
	rsp, err := client.CreateInOrder(key, string(valueJson), 30)
	if err != nil {
		logger.Errorf("[enqueueLock] failed to CreateInOrder name[%s] id[%s] error:%s", key, value, err.Error())
		return "", ErrEnqueueLock
	}

	_, token := path.Split(rsp.Node.Key)
	logger.Infof("[enqueueLock] Got token[%s] for lock[%s] with id[%s]", token, name, value)

	return token, nil
}

func waitForLock(client *etcd.Client, lockType LOCK_TYPE, namespace, name, value, token string, ttl uint64) error {
	watchChan := make(chan *etcd.Response, 1)
	watchFailChan := make(chan bool, 1)
	watchStopChan := make(chan bool, 1)

	go func() {
		logger.Infof("[waitForLock] start to watch lock[%s-%s] token[%s]", name, LockTypes[lockType], token)
		watch(client, namespace, name, watchChan, watchStopChan, watchFailChan)
	}()

	defer func() {
		logger.Infof("[waitForLock] stop to watch lock[%s-%s] token[%s]", name, LockTypes[lockType], token)
		watchStopChan <- true
	}()

	if ifGot, err := tryLock(client, lockType, namespace, name, value, token, ttl); err != nil {
		return err
	} else if ifGot {
		logger.Infof("[waitForLock] got the lock[%s-%s] token[%s]", name, LockTypes[lockType], token)
		return nil
	}

	for {
		select {
		case rsp := <-watchChan:
			if rsp == nil {
				logger.Info("[waitForLock] got nil rsp in watch channel.")
				continue
			}
			logger.Infof("[waitForLock] watch rsp action[%s] lock[%s-%s] token[%s]", rsp.Action, name, LockTypes[lockType], token)
			if rsp.Action == "expire" || rsp.Action == "delete" {
				if _, t := path.Split(rsp.Node.Key); t == token {
					logger.Errorf("[waitForLock] lack[%s] token[%s] expired", name, token)
					return ErrLockExpired
				}
				if ifGot, err := tryLock(client, lockType, namespace, name, value, token, ttl); err != nil {
					return err
				} else if ifGot {
					logger.Infof("[waitForLock] got the lock[%s-%s] token[%s]", name, LockTypes[lockType], token)
					return nil
				}
			}
		case <-watchFailChan:
			watchChan = make(chan *etcd.Response, 1)
			go watch(client, namespace, name, watchChan, watchStopChan, watchFailChan)
			//		case <-self.stop:
			//			logger.Infof("[waitForLock] stop lock.")
			//			return fmt.Errorf("Stop lock by user.")
		}
	}

	return nil
}

func tryLock(client *etcd.Client, lockType LOCK_TYPE, namespace, name, value, token string, ttl uint64) (bool, error) {
	rsp, err := client.Get(path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name), true, true)
	if err != nil {
		logger.Errorf("[tryAcquireLock] etcd get lock[%s] error: %s", name, err.Error())
		return false, ErrLockGet
	}
	for i, node := range rsp.Node.Nodes {
		var value LockValue
		if err := json.Unmarshal([]byte(node.Value), &value); err != nil {
			logger.Errorf("[tryAcquireLock] json unmarshal error: %s", err.Error())
			return false, ErrLockUnmarshal
		}
		_, t := path.Split(node.Key)
		if lockType == H_LOCK_WRITE && i == 0 {
			if value.LockType != LockTypes[H_LOCK_WRITE] || t != token {
				return false, nil
			}
			if err := refreshTTL(client, lockType, namespace, name, value.ID, token, ttl); err != nil {
				return false, err
			}
			return true, nil
		}
		if value.LockType != LockTypes[H_LOCK_READ] {
			return false, nil
		}
		if t == token {
			if err := refreshTTL(client, lockType, namespace, name, value.ID, token, ttl); err != nil {
				return false, err
			}
			return true, nil
		}
	}

	return false, ErrLockReqLost
}

func refreshTTL(client *etcd.Client, lockType LOCK_TYPE, namespace, name, value, token string, ttl uint64) error {
	valueL := &LockValue{
		LockType: LockTypes[lockType],
		ID:       value,
	}
	valueBytes, err := json.Marshal(valueL)
	if err != nil {
		logger.Errorf("[refreshTTL] failed to marshal value lock[%s] error: %s", name, err.Error())
		return ErrLockMarshal
	}
	_, err = client.Update(path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name, token), string(valueBytes), ttl)
	if err != nil {
		logger.Errorf("[refreshTTL] failed to refresh lock[%s] error: %s", name, err.Error())
		return ErrLockExpired
	}
	return nil
}

func watch(client *etcd.Client, namespace, name string, watchCh chan *etcd.Response, watchStopCh chan bool, watchFailCh chan bool) {
	_, err := client.Watch(path.Join(HAUNT_TIMING_LOCK_DIR, namespace, name), 0, true, watchCh, watchStopCh)
	if err == etcd.ErrWatchStoppedByUser {
		return
	} else {
		logger.Errorf("[watch] watch key[%s] error: %s", name, err.Error())
		watchFailCh <- true
	}
}
