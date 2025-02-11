package main

import (
	"fmt"
	"github.com/rwynn/gtm/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

func Duck(client *mongo.Client, o *ConfigOptions) {
	gtmOpts := gtm.DefaultOptions()
	gtmOpts.Ordering = gtm.AnyOrder
	gtmOpts.OpLogDisabled = false
	ctx := gtm.StartMulti([]*mongo.Client{client}, gtmOpts)
	for {
		select {
		case err := <-ctx.ErrC:
			fmt.Printf("got err %+v", err)
			break
		case op := <-ctx.OpC:
			fmt.Printf("got op %+v", op)
			break
		}
	}
}
