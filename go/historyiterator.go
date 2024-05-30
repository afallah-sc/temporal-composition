package main

import (
	"go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
)

type compositeHistoryIterator struct {
	temporalHistoryIterator client.HistoryEventIterator
	snapHistoryIterator     client.HistoryEventIterator
}

func (c compositeHistoryIterator) HasNext() bool {
	return c.temporalHistoryIterator.HasNext() || c.snapHistoryIterator.HasNext()
}

func (c compositeHistoryIterator) Next() (*history.HistoryEvent, error) {
	if c.temporalHistoryIterator.HasNext() {
		return c.temporalHistoryIterator.Next()
	}

	return c.snapHistoryIterator.Next()
}
