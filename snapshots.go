package main

type Snapshots []Snapshot

func (s Snapshots) Len() int {
	return len(s)
}

func (s Snapshots) Less(i, j int) bool {
	return s[i].EndTime.Before(s[j].EndTime)
}

func (s Snapshots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
