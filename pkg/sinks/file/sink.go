package file

import (
	"encoding/json"
	"fmt"
	"github.com/rwynn/gtm/v2"
)

type Sink struct {
	VirtualDeleteFieldName string
}

func (s Sink) process(data map[string]interface{}, isDelete bool) error {
	if s.VirtualDeleteFieldName != "" {
		if isDelete {
			data[s.VirtualDeleteFieldName] = 1
		} else {
			data[s.VirtualDeleteFieldName] = 0
		}
	}
	byteData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	// todo: write to file
	fmt.Println(string(byteData))
	return nil
}

func (s Sink) RouteData(op *gtm.Op) (err error) {
	return s.process(op.Data, false)
}

func (s Sink) RouteDelete(op *gtm.Op) (err error) {
	return s.process(op.Data, true)
}

func (s Sink) RouteDrop(op *gtm.Op) (err error) {
	return nil
}

func (s Sink) Flush() error {
	return nil
}
