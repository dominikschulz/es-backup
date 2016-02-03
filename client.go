package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

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
	for i, s := range r.Snapshots {
		if s.State == "IN_PROGRESS" {
			// delete any snapshots which are in progress
			r.Snapshots = append(r.Snapshots[:i], r.Snapshots[i+1:]...)
		}
	}
	sort.Sort(r.Snapshots)
	return r.Snapshots, nil
}

func (c *SimpleClient) CreateSnapshot() (string, error) {
	name := time.Now().UTC().Format("20060102150405")
	url := fmt.Sprintf(c.URL+"/%s/%s?wait_for_completion=true", c.Bucket, name)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return name, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return name, err
	}
	defer resp.Body.Close()

	return name, nil
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
