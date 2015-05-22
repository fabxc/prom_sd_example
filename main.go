package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"

	"github.com/coreos/go-etcd/etcd"
	"github.com/prometheus/log"
)

const (
	etcdServer     = "http://127.0.0.1:4001"
	servicesPrefix = "/services"
	targetDir      = "tgroup"
)

var (
	pathPatInfo     = regexp.MustCompile("/services/(.*)/info")
	pathPatInstance = regexp.MustCompile("/services/(.*)/instances((?:/(.*))?)")
)

// TargetGroup is the target group read by Prometheus.
type TargetGroup struct {
	Targets []string          `json:"targets,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// Instance is the instance object stored in etcd.
type Instance struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// ServiceInfo is the information object stored in etcd.
type ServiceInfo struct {
	Owner     string `json:"owner"`
	Monitored bool   `json:"monitored"`
}

// service is a service stored in etcd.
type service struct {
	info      *ServiceInfo
	instances map[string]*Instance
}

// services are the services stored in etcd.
type services struct {
	m   map[string]*service // The current services.
	del []string            // Services deleted in the last update.
}

func main() {
	flag.Parse()

	client := etcd.NewClient([]string{etcdServer})

	srvs := &services{
		m: map[string]*service{},
	}
	updates := make(chan *etcd.Response)

	// Perform an initial read of all services.
	res, err := client.Get(servicesPrefix, false, true)
	if err != nil {
		panic(err)
	}
	srvs.update(res.Node)
	srvs.persist()

	// Start watching for updates.
	go func() {
		res, err := client.Watch(servicesPrefix, 0, true, updates, nil)
		if err != nil {
			log.Errorln(err)
		}
		log.Infoln(res)
	}()

	// Apply updates sent on the channel.
	for res := range updates {
		if res.Action == "delete" {
			log.Debugf("delete: %s", res.Node.Key)
			srvs.delete(res.Node)
		} else {
			log.Debugf("%s: %s = %s", res.Action, res.Node.Key, res.Node.Value)
			srvs.update(res.Node)
		}
		srvs.persist()
	}
}

// delete services or instances based on the given node.
func (srvs *services) delete(node *etcd.Node) {
	if node.Key == "/services" {
		for name := range srvs.m {
			srvs.m[name].instances = nil
		}
		return
	}

	if pathPatInfo.MatchString(node.Key) {
		// Delete an entire service.
		name := pathPatInfo.FindStringSubmatch(node.Key)[1]
		delete(srvs.m, name)
		srvs.del = append(srvs.del, name)

	} else if pathPatInstance.MatchString(node.Key) {
		// Delete one or all instances for a service
		match := pathPatInfo.FindStringSubmatch(node.Key)
		name := match[1]

		srv, ok := srvs.m[name]
		if !ok {
			log.Errorf("instance deletion for unknown service %q", name)
			return
		}
		// The whole instaces space was deleted.
		if match[2] == "" {
			for name := range srv.instances {
				delete(srv.instances, name)
			}
		} else {
			delete(srv.instances, match[2])
		}
	} else {
		log.Errorf("cannot resolve key %q", node.Key)
	}
}

// update the services based on the given node.
func (srvs *services) update(node *etcd.Node) {
	if node.Dir {
		for _, n := range node.Nodes {
			srvs.update(n)
		}
		return
	}

	if pathPatInfo.MatchString(node.Key) {
		var info *ServiceInfo
		err := json.Unmarshal([]byte(node.Value), &info)
		if err != nil {
			log.Errorln(err)
			return
		}
		name := pathPatInfo.FindStringSubmatch(node.Key)[1]
		srv, ok := srvs.m[name]
		if !ok {
			srv = &service{instances: map[string]*Instance{}}
			srvs.m[name] = srv
		}
		srv.info = info

	} else if pathPatInstance.MatchString(node.Key) {
		match := pathPatInstance.FindStringSubmatch(node.Key)
		name := match[1]

		srv, ok := srvs.m[match[1]]
		if !ok {
			log.Errorf("instance update for unknown service %q", name)
			return
		}
		var inst *Instance
		err := json.Unmarshal([]byte(node.Value), &inst)
		if err != nil {
			log.Errorln(err)
			return
		}
		srv.instances[match[2]] = inst
	} else {
		log.Errorf("cannot resolve key %q", node.Key)
	}
}

/// persist writes the current services to disc.
func (srvs *services) persist() {
	for name, srv := range srvs.m {
		if !srv.info.Monitored {
			continue
		}
		tg := &TargetGroup{}
		for _, inst := range srv.instances {
			tg.Targets = append(tg.Targets, fmt.Sprintf("%s:%d", inst.Host, inst.Port))
		}
		content, err := json.Marshal([]*TargetGroup{tg})
		if err != nil {
			log.Errorln(err)
			continue
		}

		f, err := os.Create("tgroups/" + name + ".json")
		if err != nil {
			log.Errorln(err)
			continue
		}
		if _, err := f.Write(content); err != nil {
			log.Errorln(err)
		}
		f.Close()
	}
	// Remove files for disappeared services.
	for _, name := range srvs.del {
		os.Remove("tgroups/" + name + ".json")
	}
	srvs.del = nil
}
