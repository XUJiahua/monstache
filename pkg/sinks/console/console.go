package console

import (
	"fmt"
	"github.com/rwynn/gtm/v2"
)

type Sink struct {
}

func (sink Sink) RouteDrop(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Sink) RouteData(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Sink) RouteDelete(op *gtm.Op) (err error) {
	fmt.Println(op)
	return nil
}

func (sink Sink) Flush() error {
	return nil
}
