package console

import (
	"fmt"
	"github.com/rwynn/gtm/v2"
)

type Config struct {
	Enabled bool `toml:"enabled"`
}

type Client struct {
}

func (sink Client) RouteDrop(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Client) RouteData(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Client) RouteDelete(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Client) Flush() error {
	return nil
}
