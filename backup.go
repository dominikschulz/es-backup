package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
)

func backupAndRemove(c *Config) error {
	client := SimpleClient{
		URL:    c.URL(),
		Bucket: c.Name,
	}

	log.Debugf("Fetching snapshots ...")
	snaps, err := client.ListSnapshots()
	if err != nil {
		return err
	}

	for d, s := range snaps {
		log.Debugf("Snapshot #%d: %s", d, s)
	}

	needSnap := false
	nextSnap := time.Now()
	if len(snaps) < 1 {
		needSnap = true
	} else {
		lastSnap := snaps[len(snaps)-1]
		nextSnap = lastSnap.EndTime.Add(time.Duration(c.Interval) * time.Hour)
		needSnap = nextSnap.Before(time.Now())
	}
	if needSnap {
		log.Debugf("Creating snapshot ...")
		name, err := client.CreateSnapshot()
		if err != nil {
			return err
		}
		log.Infof("Created Snapshot %s", name)
	} else {
		log.Debugf("Not creating snapshot. Next is due at %s", nextSnap.Format(time.RFC3339))
	}

	keepTS := time.Now().Add(-24 * time.Hour * time.Duration(c.Retention))
	for _, v := range snaps {
		if v.EndTime.Before(keepTS) && v.State != "IN_PROGRESS" {
			err := client.DeleteSnapshot(v.Snapshot)
			if err != nil {
				log.Warnf("Failed to delete snapshot %s: %s", v, err)
			} else {
				log.Infof("Deleted snapshot %s", v)
			}
		}
	}
	return nil
}
