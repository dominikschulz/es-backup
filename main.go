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

	log.Debugf("Creating snapshot ...")
	err := client.CreateSnapshot()
	if err != nil {
		return err
	}

	victims, err := client.ListSnapshots()
	if err != nil {
		return err
	}

	for i := len(victims) - (c.Retention + 1); i >= 0; i-- {
		iname := victims[i]
		err := client.DeleteSnapshot(iname.Snapshot)
		if err != nil {
			log.Printf("Failed to delete snapshot %s: %s", iname, err)
		} else {
			log.Printf("Deleted snapshot %s", iname)
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
	interval := time.Hour * time.Duration(cfg.Interval)
	for {
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
			log.Warnf("Failed to delete snapshots: %s", err)
			continue
		}
		if interval < time.Second {
			break
		}
		log.Infof("Waiting %s until next run", interval.String())
		time.Sleep(interval)
	}
	os.Exit(0)
}
