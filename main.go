package main

import (
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
	"github.com/prometheus/client_golang/prometheus"
)

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
		log.Infof("Attempting Snapshot ...")
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
