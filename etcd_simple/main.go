package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"

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
	srvs.handle(res.Node, srvs.update)
	srvs.persist()

	// Start watching for updates.
	go func() {
		_, err := client.Watch(servicesPrefix, 0, true, updates, nil)
		if err != nil {
			log.Errorln(err)
		}
	}()

	// Apply updates sent on the channel.
	for res := range updates {
		log.Infoln(res.Action, res.Node.Key, res.Node.Value)

		h := srvs.update
		if res.Action == "delete" {
			h = srvs.delete
		}
		srvs.handle(res.Node, h)
		srvs.persist()
	}
}

// handle recursively applies the handler h to the nodes in the subtree
// represented by node.
func (srvs *services) handle(node *etcd.Node, h func(*etcd.Node)) {
	if pathPat.MatchString(node.Key) {
		h(node)
	} else {
		log.Warnf("unhandled key %q", node.Key)
	}

	if node.Dir {
		for _, n := range node.Nodes {
			srvs.handle(n, h)
		}
	}
}

// update the services based on the given node.
func (srvs *services) update(node *etcd.Node) {
	match := pathPat.FindStringSubmatch(node.Key)
	// Creating a new job dir does not require any action.
	if match[2] == "" {
		return
	}
	srv := match[1]

	instances, ok := srvs.m[srv]
	if !ok {
		instances = Instances{}
	}
	instances[match[2]] = node.Value
	srvs.m[srv] = instances
}

// delete services or instances based on the given node.
func (srvs *services) delete(node *etcd.Node) {
	match := pathPat.FindStringSubmatch(node.Key)
	srv := match[1]

	// Deletion of an entire service.
	if match[2] == "" {
		delete(srvs.m, srv)
		return
	}

	// Delete the instance from the service.
	delete(srvs.m[srv], match[2])
}

// persist writes the current services to disc.
func (srvs *services) persist() {
	// Write files for current services.
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

	// Remove files for services that no longer exist.
	filepath.Walk(*targetDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			job := strings.SplitN(filepath.Base(path), ".", 2)[0]
			// Remove file if associated job does no longer exist.
			if _, ok := srvs.m[job]; !ok {
				if err := os.Remove(path); err != nil {
					log.Errorln(err)
				}
			}
		}
		return nil
	})
}
