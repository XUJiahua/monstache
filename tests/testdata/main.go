package main

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

// Define a struct that represents your data model
type ExampleStruct struct {
	Field1     string    `bson:"field1"`
	Field2     int       `bson:"field2"`
	UpdateTime time.Time `bson:"update_time"`
}

func main() {
	// Set client options and connect to MongoDB
	clientOptions := options.Client().ApplyURI("mongodb://10.30.11.112:27017,10.30.11.112:27018,10.30.11.112:27019")
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// Get a handle for your collection
	collection := client.Database("db1").Collection("col1")

	// Create an instance of the struct
	exampleData := ExampleStruct{
		Field1:     "value1",
		Field2:     123,
		UpdateTime: time.Now(),
	}

	// Insert the data
	insertResult, err := collection.InsertOne(context.TODO(), exampleData)
	if err != nil {
		log.Fatal(err)
	}

	// Print the ID of the inserted document
	log.Println("Inserted a single document: ", insertResult.InsertedID)

	// Close the connection once no longer needed
	err = client.Disconnect(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
}
