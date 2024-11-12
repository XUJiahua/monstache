package file

import (
	"context"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/sirupsen/logrus"
)

type Config struct {
	Enabled bool `toml:"enabled"`
}

type Client struct {
}

func (s Client) EmbedDoc() bool {
	return false
}
func (s Client) Name() string {
	return "file"
}

func (s Client) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	// now as a black hole
	logrus.Debugf("%d requests processed", len(requests))
	return nil
}
