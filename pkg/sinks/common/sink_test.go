package common

import (
	"fmt"
	"github.com/rwynn/monstache/v6/pkg/sinks/kafka"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func init() {
	logrus.SetLevel(logrus.DebugLevel)
}

func TestNew(t *testing.T) {
	p, err := kafka.NewKafkaProducer("10.30.11.112:9092", func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	}, func(s string, i ...interface{}) {
		fmt.Printf(s, i...)
		fmt.Println()
	})
	require.NoError(t, err)

	sink, err := New(p, nil, SinkConfig{})
	require.NoError(t, err)

	time.Sleep(time.Second * 11)

	sink.Close()
}
