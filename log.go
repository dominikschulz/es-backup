package main

import (
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
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
