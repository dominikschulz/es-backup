package main

import (
	"fmt"
	"time"
)

type Snapshot struct {
	Snapshot string    `json:"snapshot"`
	State    string    `json:"state"`
	EndTime  time.Time `json:"end_time"`
}

func (s Snapshot) String() string {
	return fmt.Sprintf("%s (finished at %s with %s)", s.Snapshot, s.EndTime.Format(time.RFC3339), s.State)
}
