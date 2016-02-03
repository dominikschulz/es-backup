package main

import (
	"fmt"

	"github.com/alexflint/go-arg"
	"github.com/danryan/env"
)

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
