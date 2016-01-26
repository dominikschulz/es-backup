package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alexflint/go-arg"
	"github.com/danryan/env"
)

func init() {
	lvlStr := os.Getenv("LOGLEVEL")
	if lvlStr != "" {
		lvl, err := log.ParseLevel(strings.ToLower(lvlStr))
		if err == nil {
			log.SetLevel(lvl)
		}
	}
}

type SimpleClient struct {
	URL    string
	Bucket string
	http   http.Client
}

func (c *SimpleClient) ListSnapshots() ([]Snapshot, error) {
	var r SnapshotResponse
	url := fmt.Sprintf(c.URL+"/%s/_all", c.Bucket)
	resp, err := http.Get(url)
	if err != nil {
		return r.Snapshots, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&r)
	if err != nil {
		return r.Snapshots, err
	}
	sort.Sort(r.Snapshots)
	return r.Snapshots, nil
}

func (c *SimpleClient) CreateSnapshot() error {
	url := fmt.Sprintf(c.URL+"/%s/%s?wait_for_completion=true", c.Bucket, time.Now().Format("20060102150405"))
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *SimpleClient) DeleteSnapshot(name string) error {
	url := fmt.Sprintf(c.URL+"/%s/%s", c.Bucket, name)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

type Snapshots []Snapshot

type Snapshot struct {
	Snapshot string    `json:"snapshot"`
	State    string    `json:"state"`
	EndTime  time.Time `json:"end_time"`
}

func (s Snapshot) String() string {
	return fmt.Sprintf("%s (finished at %s with %s)", s.Snapshot, s.EndTime.Format(time.RFC3339), s.State)
}

func (s Snapshots) Len() int {
	return len(s)
}

func (s Snapshots) Less(i, j int) bool {
	return s[i].EndTime.Before(s[j].EndTime)
}

func (s Snapshots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SnapshotResponse struct {
	Snapshots Snapshots `json:"snapshots"`
}

type Config struct {
	Host      string `env:"key=HOST default=localhost:9200" arg:"--host"`
	Retention int    `env:"key=KEEP default=7" arg:"--keep"`
	Name      string `env:"key=NAME default=backup" arg:"--name"`
	Interval  int    `env:"key=INTERVAL default=24" arg:"--interval"`
}

func New() (*Config, error) {
	c := Config{}
	if err := env.Process(&c); err != nil {
		return nil, err
	}
	arg.MustParse(&c)
	return &c, nil
}

func (c *Config) URL() string {
	return fmt.Sprintf("http://%s/_snapshot", c.Host)
}

func backupAndRemove(c *Config) error {
	client := SimpleClient{
		URL:    c.URL(),
		Bucket: c.Name,
	}

	log.Debugf("Fetching snapshot ...")
	snaps, err := client.ListSnapshots()
	if err != nil {
		return err
	}

	for d, s := range snaps {
		log.Debugf("Snapshot #%d: %s", d, s)
	}

	lastSnap := snaps[len(snaps)-1]
	nextSnap := lastSnap.EndTime.Add(time.Duration(c.Interval) * time.Hour)
	if len(snaps) < 1 || nextSnap.Before(time.Now()) {
		log.Debugf("Creating snapshot ...")
		err = client.CreateSnapshot()
		if err != nil {
			return err
		}
	} else {
		log.Debugf("Not creating snapshot. Next is due at %s", nextSnap.Format(time.RFC3339))
	}

	keepTS := time.Now().Add(-24 * time.Hour * time.Duration(c.Retention))
	for _, v := range snaps {
		if v.EndTime.Before(keepTS) {
			err := client.DeleteSnapshot(v.Snapshot)
			if err != nil {
				log.Printf("Failed to delete snapshot %s: %s", v, err)
			} else {
				log.Printf("Deleted snapshot %s", v)
			}
		}
	}
	return nil
}

func main() {
	cfg, err := New()
	if err != nil {
		log.Fatalf("Failed to parse config: %s", err)
		return
	}

	runs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "elasticsearch_backup_runs_total",
			Help: "Number of elasticsearch backup runs",
		},
		[]string{"status"},
	)
	runs = prometheus.MustRegisterOrGet(runs).(*prometheus.CounterVec)
	duration := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "elasticsearch_backup_duration",
			Help: "Duration of elasticsearch backup runs",
		},
		[]string{"operation"},
	)
	duration = prometheus.MustRegisterOrGet(duration).(*prometheus.SummaryVec)

	go listen()

	interval := time.Hour * time.Duration(cfg.Interval)
	for {
		t0 := time.Now()
		opFunc := func() error {
			return backupAndRemove(cfg)
		}
		logFunc := func(err error, wait time.Duration) {
			log.Warnf("Failed to connect to ES: %s. Retry in %s", err, wait)
		}
		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = time.Second
		bo.MaxInterval = 60 * time.Second
		bo.MaxElapsedTime = 15 * time.Minute
		err := backoff.RetryNotify(opFunc, bo, logFunc)
		if err != nil {
			runs.WithLabelValues("failed").Inc()
			log.Warnf("Failed to delete snapshots: %s", err)
			continue
		}
		runs.WithLabelValues("ok").Inc()
		d0 := float64(time.Since(t0)) / float64(time.Microsecond)
		duration.WithLabelValues("backup").Observe(d0)

		if interval < time.Second {
			break
		}
		log.Infof("Waiting %s until next run", interval.String())
		time.Sleep(interval)
	}
	os.Exit(0)
}

func listen() {
	listen := os.Getenv("LISTEN")
	if listen == "" {
		listen = ":8080"
	}
	s := &http.Server{
		Addr:    listen,
		Handler: requestHandler(),
	}
	log.Printf("Listening on %s", listen)
	log.Errorf("Failed to listen on %s: %s", listen, s.ListenAndServe())
}

func requestHandler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", prometheus.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "OK", http.StatusOK)
	})
	mux.HandleFunc("/", http.NotFound)
	return mux
}
