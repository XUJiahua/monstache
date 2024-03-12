package kafka

import (
	"context"
	"fmt"
	"github.com/rwynn/monstache/v6/pkg/sinks/bulk"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"testing"
)

func TestKafkaProducer_Produce(t *testing.T) {
	p, err := NewKafkaProducer("10.30.11.112:9092", func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	}, func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	})
	require.NoError(t, err)
	err = p.Produce("b", []byte("a"), []byte("b"))
	require.NoError(t, err)
	err = p.Close()
	require.NoError(t, err)
}

func BenchmarkProduce(b *testing.B) {
	var infoLog = log.New(os.Stdout, "INFO ", log.Flags())
	var errorLog = log.New(os.Stderr, "ERROR ", log.Flags())
	producer, err := NewKafkaProducer("10.30.11.112:9092", func(s string, i ...interface{}) {
		infoLog.Printf(s, i...)
	}, func(s string, i ...interface{}) {
		errorLog.Printf(s, i...)
	})
	require.NoError(b, err)

	for n := 0; n < b.N; n++ {
		// fixme: too slow
		err := producer.Produce("topic", []byte("a"), []byte("b"))
		require.NoError(b, err)
	}
}

func TestKafkaProducer_ProduceBatch(t *testing.T) {
	p := getInstance(t)

	// produce two message which belongs to different topics
	err := p.ProduceBatch(context.TODO(), kafka.Message{
		Topic: "monstache.db1.col1",
		Key:   []byte("a"),
		Value: []byte("a"),
	}, kafka.Message{
		Topic: "b.topic",
		Value: []byte("b"),
	})
	require.NoError(t, err)
}

func getInstance(t *testing.T) *KafkaProducer {
	p, err := NewKafkaProducer("10.30.11.112:9092", func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	}, func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	})
	require.NoError(t, err)
	return p
}

func TestKafkaProducer_Commit(t *testing.T) {
	p := getInstance(t)
	err := p.Commit(context.TODO(), []bulk.BulkableRequest{Request{
		namespace: "db1.col1",
		id:        "a",
		doc:       "a",
	}})
	require.NoError(t, err)
}
