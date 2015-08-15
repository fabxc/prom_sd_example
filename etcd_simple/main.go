package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"

	"github.com/coreos/go-etcd/etcd"
	"github.com/prometheus/log"
)

const servicesPrefix = "/services"

var pathPat = regexp.MustCompile(`/services/([^/]+)(?:/(\d+))?`)

// TargetGroup is the target group read by Prometheus.
type TargetGroup struct {
	Targets []string          `json:"targets,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type Instances map[string]string

// services are the services stored in etcd.
type services struct {
	m   map[string]Instances // The current services.
	del []string             // Services deleted in the last update.
}

var (
	etcdServer = flag.String("server", "http://127.0.0.1:4001", "etcd server to connect to")
	targetDir  = flag.String("target-dir", "tgroups", "directory to store the target group files")
)

func main() {
	flag.Parse()

	client := etcd.NewClient([]string{*etcdServer})

	srvs := &services{
		m: map[string]Instances{},
	}
	updates := make(chan *etcd.Response)

	// Perform an initial read of all services.
	res, err := client.Get(servicesPrefix, false, true)
	if err != nil {
		log.Fatalf("Error on initial retrieval: %s", err)
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
		if !pathPat.MatchString(res.Node.Key) {
			log.Warnf("unhandled key %q", res.Node.Key)
			continue
		}
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
	if node.Dir {
		for _, n := range node.Nodes {
			srvs.delete(n)
		}
		return
	}

	match := pathPat.FindStringSubmatch(node.Key)
	srv := match[1]
	// Deletion of an entire service.
	if match[2] == "" {
		srvs.del = append(srvs.del, srv)
		delete(srvs.m, srv)
		return
	}

	instances, ok := srvs.m[srv]
	if !ok {
		log.Errorf("Received delete for unknown service %s", srv)
		return
	}
	delete(instances, match[2])
}

// update the services based on the given node.
func (srvs *services) update(node *etcd.Node) {
	if node.Dir {
		for _, n := range node.Nodes {
			srvs.update(n)
		}
		return
	}

	match := pathPat.FindStringSubmatch(node.Key)
	srv := match[1]
	// Creating a new job dir does not require an action.
	if match[2] == "" {
		return
	}

	instances, ok := srvs.m[srv]
	if !ok {
		instances = Instances{}
	}
	instances[match[2]] = node.Value
	srvs.m[srv] = instances
}

// persist writes the current services to disc.
func (srvs *services) persist() {
	for job, instances := range srvs.m {
		var targets []string
		for _, addr := range instances {
			targets = append(targets, addr)
		}
		content, err := json.Marshal([]*TargetGroup{
			{
				Targets: targets,
				Labels:  map[string]string{"job": job},
			},
		})
		if err != nil {
			log.Errorln(err)
			continue
		}

		f, err := create(filepath.Join(*targetDir, job+".json"))
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
	for _, job := range srvs.del {
		if err := os.Remove(filepath.Join(*targetDir, job+".json")); err != nil {
			log.Errorln(err)
		}
	}
	srvs.del = nil
}
