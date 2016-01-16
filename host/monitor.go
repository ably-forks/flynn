package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/host/fixer"
	"github.com/flynn/flynn/pkg/cluster"
)

type MonitorMetadata struct {
	Enabled bool `json:"enabled,omitempty"`
	Hosts   int  `json:"hosts,omitempty"`
}

type Monitor struct {
	addr       string
	dm         *DiscoverdManager
	discoverd  *discoverdWrapper
	discClient *discoverd.Client
	isLeader   bool
	c          *cluster.Client
	l          log15.Logger
	hostCount  int
}

func NewMonitor(dm *DiscoverdManager, addr string) *Monitor {
	return &Monitor{
		dm:        dm,
		discoverd: nil,
		addr:      addr,
	}
}

func (m *Monitor) Run() {
	m.l = log15.New()
	for {
		if m.dm.localConnected() {
			m.discClient = discoverd.NewClient()
			break
		}
		time.Sleep(1 * time.Second)
		m.l.Info("waiting for local discoverd to come up")
	}

	m.l.Info("waiting for raft leader")

	for {
		_, err := m.discClient.RaftLeader()
		if err == nil {
			break // leader is now elected
		}
		m.l.Info("failed to get raft leader: %s", err.Error())
		time.Sleep(10 * time.Second)
	}

	m.l.Info("raft leader up, connecting cluster client")
	// we can also connect the leader election wrapper now
	m.discoverd = newDiscoverdWrapper(m.addr + ":1113")
	// connect cluster client now that discoverd is up.
	m.c = cluster.NewClient()

	monitorSvc := discoverd.NewService("cluster-monitor")

	m.l.Info("waiting for monitor service to be enabled for this cluster")

	for {
		monitorMeta, err := monitorSvc.GetMeta()
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		var decodedMeta MonitorMetadata
		if err := json.Unmarshal(monitorMeta.Data, &decodedMeta); err != nil {
			fmt.Printf("error decoding cluster-monitor meta")
			continue
		}
		if decodedMeta.Enabled {
			m.hostCount = decodedMeta.Hosts
			break
		}
		time.Sleep(5 * time.Second)
	}

	m.l.Info("attempting to register cluster-monitor")
	for {
		isLeader, err := m.discoverd.Register()
		if err == nil {
			m.isLeader = isLeader
			break
		}
		m.l.Info("error registering cluster-monitor %s", err.Error())
		time.Sleep(5 * time.Second)
	}
	leaderCh := m.discoverd.LeaderCh()

	m.l.Info("starting monitor loop")
	for {
		var isLeader bool
		select {
		case isLeader = <-leaderCh:
			m.isLeader = isLeader
			continue
		default:
		}
		if m.isLeader {
			m.l.Info("we are the leader")
			hosts, err := m.c.Hosts()
			if err != nil || len(hosts) < m.hostCount {
				m.l.Info("waiting for hosts attempting ressurection/fixing", "current", len(hosts), "want", m.hostCount)
				time.Sleep(10 * time.Second)
				continue
			}
			m.l.Info("required hosts online, proceding to check cluster")
			f := fixer.NewClusterFixer(hosts, m.c, m.l)
			if err := f.FixFlannel(); err != nil {
				m.l.Error("error fixing flannel, retrying")
				time.Sleep(10 * time.Second)
				continue
			}

			m.l.Info("checking the controller api")
			controllerService := discoverd.NewService("controller")
			controllerInstances, _ := controllerService.Instances()
			if len(controllerInstances) > 0 {
				m.l.Info("found running controller api instances", "n", len(controllerInstances))
				if err := f.FixController(controllerInstances, false); err != nil {
					// TODO: give this a deadline, after a certain point of waiting we will kill the schedulers and fix everything ourselves
					m.l.Error("error fixing controller, will retry until deadline is reached then attempt hard repair", "err", err)
					// XXX: goes straight to hard repair, we should retry until deadline instead
					if err := f.KillSchedulers(); err != nil {
						m.l.Error("error killing schedulers, retrying", "err", err)
						time.Sleep(10 * time.Second)
						continue
					}
				}
			}

			if err := f.FixPostgres(); err != nil {
				m.l.Error("error when fixing postgres, retrying", "err", err)
				time.Sleep(10 * time.Second)
				continue
			}

			m.l.Info("checking for running controller API")
			controllerInstances, _ = controllerService.Instances()
			if len(controllerInstances) == 0 {
				if err := f.KillSchedulers(); err != nil {
					m.l.Error("error killing schedulers, retrying", "err", err)
					time.Sleep(10 * time.Second)
					continue
				}
				controllerInstances, err = f.StartAppJob("controller", "web", "controller")
				if err != nil {
					m.l.Error("error starting controller api, retrying", "err", err)
					time.Sleep(10 * time.Second)
					continue
				}
			} else {
				m.l.Info("found running controller API instances", "n", len(controllerInstances))
			}
			if err := f.FixController(controllerInstances, true); err != nil {
				m.l.Error("error fixing controller, retrying", "err", err)
				time.Sleep(10 * time.Second)
				continue
			}
			m.l.Info("cluster currently healthy")
		}
		m.l.Info("no more to do right now, sleeping")
		time.Sleep(10 * time.Second)
	}
}
