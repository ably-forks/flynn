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
	deadline   time.Time
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
		// TODO(jpg): Use ticker here
		if m.isLeader {
			var faulted bool
			m.l.Info("we are the leader")
			hosts, err := m.c.Hosts()
			if err != nil || len(hosts) < m.hostCount {
				m.l.Info("waiting for hosts attempting ressurection/fixing", "current", len(hosts), "want", m.hostCount)
				time.Sleep(10 * time.Second)
				continue
			}
			m.l.Info("required hosts online, proceding to check cluster")

			m.l.Info("checking the controller api")
			controllerService := discoverd.NewService("controller")
			controllerInstances, _ := controllerService.Instances()
			if len(controllerInstances) > 0 {
				m.l.Info("found running controller api instances", "n", len(controllerInstances))
			} else {
				m.l.Error("did not find any controller api instances")
				faulted = true
			}

			if _, err := discoverd.NewService("controller-scheduler").Leader(); err != nil && !discoverd.IsNotFound(err) {
				m.l.Error("error getting scheduler leader")
			} else if err == nil {
				m.l.Info("scheduler is up")
			} else {
				m.l.Error("scheduler is not up")
				faulted = true
			}

			if faulted && m.deadline.IsZero() {
				m.l.Error("cluster is unhealthy, setting fault")
				m.deadline = time.Now().Add(60 * time.Second)
			} else if !faulted && !m.deadline.IsZero() {
				m.l.Info("cluster currently healthy, clearing fault")
				m.deadline = time.Time{}
			}

			if !m.deadline.IsZero() && time.Now().After(m.deadline) {
				m.l.Error("fault deadline reached")
				m.Repair()
			}
		}
		m.l.Info("no more to do right now, sleeping")
		time.Sleep(10 * time.Second)
	}
}

func (m *Monitor) Repair() error {
	m.l.Info("initiating cluster repair")
	hosts, err := m.c.Hosts()
	if err != nil {
		return err
	}
	f := fixer.NewClusterFixer(hosts, m.c, m.l)
	// killing the schedulers to prevent interference
	f.KillSchedulers()
	// ensure postgres is working
	f.FixPostgres()
	// ensure controller api is working
	controllerService := discoverd.NewService("controller")
	controllerInstances, _ := controllerService.Instances()
	if len(controllerInstances) == 0 {
		controllerInstances, err = f.StartAppJob("controller", "web", "controller")
		if err != nil {
			return err
		}
	}
	// fix any formations and start the scheduler again
	if err := f.FixController(controllerInstances, true); err != nil {
		return err
	}
	// zero out the deadline timer
	m.deadline = time.Time{}
	return nil
}
