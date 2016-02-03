package main

import (
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
)

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
