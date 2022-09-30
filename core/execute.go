package core

import (
	cfg "project/config"
)

type Executor struct {
	Context *cfg.Context
}

func NewExecutor(c *Coordinator) *Executor {
	return &Executor{
		Context: c.context,
	}
}
