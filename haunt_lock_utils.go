//        file: haunt_lock/haunt_lock_utils.go
// description: Utility for haunt lock of etcd.

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package haunt_lock

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync/atomic"

	"github.com/coreos/go-etcd/etcd"
)

const (
	ErrCodeETCDKeyNotFound = 100
)

var (
	lockID   int64
	pid      int
	hostname string
	ip       string
)

func NewEtcdClient(etcdHost string) *etcd.Client {
	hosts := initEtcdHost(etcdHost)
	return etcd.NewClient(hosts)
}

func GetMasterDir() string {
	return HAUNT_MASTER_DIR
}

func initEtcdHost(etcdHost string) []string {
	hosts := strings.Split(etcdHost, ",")
	for i, host := range hosts {
		u, err := url.Parse(host)
		if err != nil {
			return nil
		}
		if u.Scheme == "" {
			u.Scheme = "http"
		}
		hosts[i] = u.String()
	}
	return hosts
}

func IfETCDKeyNotFound(err error) bool {
	etcdErr, ok := err.(*etcd.EtcdError)
	return ok && etcdErr != nil && etcdErr.ErrorCode == ErrCodeETCDKeyNotFound
}

func getID() string {
	return fmt.Sprintf("%d:%d:%s:%s", atomic.AddInt64(&lockID, 1), pid, hostname, ip)
}

func init() {
	lockID = 0
	pid = os.Getpid()
	hostName, err := os.Hostname()
	if err != nil {
		fmt.Println("Get hostname error:", err.Error())
		return
	}
	hostname = hostName
	ipAddress, err := net.ResolveIPAddr("ip", hostname)
	if err != nil {
		fmt.Println("Get IPAddress error:", err.Error())
		return
	}
	ip = ipAddress.String()
}
