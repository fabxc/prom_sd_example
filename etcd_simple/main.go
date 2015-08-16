package main

import (
	"encoding/json"
	"flag"
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

type (
	instances map[string]string
	services  map[string]instances
)

var (
	etcdServer = flag.String("server", "http://127.0.0.1:4001", "etcd server to connect to")
	targetFile = flag.String("target-file", "tgroups.json", "the file that contains the target groups")
)

func main() {
	flag.Parse()

	var (
		client  = etcd.NewClient([]string{*etcdServer})
		srvs    = services{}
		updates = make(chan *etcd.Response)
	)

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
func (srvs services) handle(node *etcd.Node, h func(*etcd.Node)) {
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
func (srvs services) update(node *etcd.Node) {
	match := pathPat.FindStringSubmatch(node.Key)
	// Creating a new job dir does not require any action.
	if match[2] == "" {
		return
	}
	srv := match[1]

	insts, ok := srvs[srv]
	if !ok {
		insts = instances{}
	}
	insts[match[2]] = node.Value
	srvs[srv] = insts
}

// delete services or instances based on the given node.
func (srvs services) delete(node *etcd.Node) {
	match := pathPat.FindStringSubmatch(node.Key)
	srv := match[1]

	// Deletion of an entire service.
	if match[2] == "" {
		delete(srvs, srv)
		return
	}

	// Delete the instance from the service.
	delete(srvs[srv], match[2])
}

// persist writes the current services to disc.
func (srvs services) persist() {
	var tgroups []*TargetGroup
	// Write files for current services.
	for job, instances := range srvs {
		var targets []string
		for _, addr := range instances {
			targets = append(targets, addr)
		}

		tgroups = append(tgroups, &TargetGroup{
			Targets: targets,
			Labels:  map[string]string{"job": job},
		})
	}

	content, err := json.Marshal(tgroups)
	if err != nil {
		log.Errorln(err)
		return
	}

	f, err := create(*targetFile)
	if err != nil {
		log.Errorln(err)
		return
	}
	defer f.Close()

	if _, err := f.Write(content); err != nil {
		log.Errorln(err)
	}
}
