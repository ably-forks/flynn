package main

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ably-forks/flynn/controller/testutils"
	"github.com/ably-forks/flynn/controller/utils"
	"github.com/ably-forks/flynn/host/types"
	"github.com/ably-forks/flynn/pkg/stream"
	"github.com/inconshreveable/log15"
)

type Host struct {
	ID       string            `json:"id"`
	Tags     map[string]string `json:"tags"`
	Healthy  bool              `json:"healthy"`
	Checks   int               `json:"checks"`
	Shutdown bool              `json:"shutdown"`

	client   utils.HostClient
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	logger   log15.Logger
}

func NewHost(h utils.HostClient, l log15.Logger) *Host {
	return &Host{
		ID:      h.ID(),
		Tags:    h.Tags(),
		Healthy: true,
		client:  h,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		logger:  l,
	}
}

func (h *Host) TagsEqual(tags map[string]string) bool {
	if len(h.Tags) != len(tags) {
		return false
	}
	for k, v := range h.Tags {
		if w, ok := tags[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// StreamEventsTo streams all job events from the host to the given channel in
// a goroutine, returning the current list of active jobs.
func (h *Host) StreamEventsTo(ch chan *host.Event) (map[string]host.ActiveJob, error) {
	log := h.logger.New("fn", "StreamEventsTo", "host.id", h.ID)
	var events chan *host.Event
	var stream stream.Stream
	connect := func() (err error) {
		log.Info("connecting job event stream")
		events = make(chan *host.Event)
		stream, err = h.client.StreamEvents("all", events)
		if err != nil {
			log.Error("error connecting job event stream", "err", err)
		}
		return
	}
	if err := connect(); err != nil {
		return nil, err
	}

	log.Info("getting active jobs")
	jobs, err := h.client.ListJobs()
	if err != nil {
		log.Error("error getting active jobs", "err", err)
		return nil, err
	}
	log.Info(fmt.Sprintf("got %d active job(s) for host %s", len(jobs), h.ID))

	go func() {
		defer stream.Close()
		defer close(h.done)
		for {
		eventLoop:
			for {
				select {
				case event, ok := <-events:
					if !ok {
						break eventLoop
					}
					ch <- event

					// if the host is a FakeHostClient with TestEventHook
					// set, send on the channel to synchronize with tests
					if h, ok := h.client.(*testutils.FakeHostClient); ok && h.TestEventHook != nil {
						h.TestEventHook <- struct{}{}
					}
				case <-h.stop:
					return
				}
			}

			log.Warn("job event stream disconnected", "err", stream.Err())
			// keep trying to reconnect, unless we are told to stop
		retryLoop:
			for {
				select {
				case <-h.stop:
					return
				default:
				}

				if err := connect(); err == nil {
					break retryLoop
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	return jobs, nil
}

func (h *Host) Close() {
	h.stopOnce.Do(func() {
		close(h.stop)
		<-h.done
	})
}

// sortHosts sorts Hosts lexicographically based on their ID
type sortHosts []*Host

func (s sortHosts) Len() int           { return len(s) }
func (s sortHosts) Less(i, j int) bool { return s[i].ID < s[j].ID }
func (s sortHosts) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sortHosts) Sort()              { sort.Sort(s) }
