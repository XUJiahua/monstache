package main

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_saveNamespaceDirectReadOffset(t *testing.T) {
	client, err := dialMongo()
	require.NoError(t, err)
	col := client.Database("db1").Collection("trans")
	result := col.FindOne(context.TODO(), bson.M{})
	require.NoError(t, result.Err())
	doc := make(map[string]interface{})
	require.NoError(t, result.Decode(&doc))
	fmt.Printf("%v", doc)
	err = saveNamespaceDirectReadOffset(client, "monstache", "exe_123", "db1.trans", doc["_id"])
	require.NoError(t, err)
}

func Test_getNamespaceDirectReadOffsets(t *testing.T) {
	client, err := dialMongo()
	require.NoError(t, err)
	offsets, err := getNamespaceDirectReadOffsets(client, "monstache", "exe_123")
	require.NoError(t, err)
	fmt.Printf("%v", offsets)
}
