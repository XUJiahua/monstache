package file

import (
	"context"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
)

type Config struct {
	Enabled bool
}

type Client struct {
}

func (s Client) Commit(ctx context.Context, requests []bulk.BulkableRequest) error {
	//TODO implement me
	panic("implement me")
}
