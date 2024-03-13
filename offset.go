package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const ConfigColResumableDirectreads = "resumable_directreads"

type resumable struct {
	NS     string      `bson:"ns"`
	Offset interface{} `bson:"offset"`
}

func getNamespaceDirectReadOffsets(client *mongo.Client, configDatabaseName string, resumeName string) (map[string]interface{}, error) {
	col := client.Database(configDatabaseName).Collection(ConfigColResumableDirectreads)
	cursor, err := col.Find(context.TODO(), bson.M{
		"_id": resumeName,
	})
	if err != nil {
		return nil, err
	}

	offsets := make(map[string]interface{})
	for cursor.Next(context.TODO()) {
		var resumable resumable
		if err = cursor.Decode(&resumable); err != nil {
			return nil, err
		}
		offsets[resumable.NS] = resumable.Offset
	}
	return offsets, nil
}

func saveNamespaceDirectReadOffset(client *mongo.Client, configDatabaseName string, resumeName string, ns string, offset interface{}) error {
	col := client.Database(configDatabaseName).Collection(ConfigColResumableDirectreads)
	doc := map[string]interface{}{
		"offset": offset,
	}
	opts := options.Update()
	opts.SetUpsert(true)
	_, err := col.UpdateOne(context.TODO(), bson.M{
		"_id": resumeName,
		"ns":  ns,
	}, bson.M{
		"$set": doc,
	}, opts)
	return err
}
