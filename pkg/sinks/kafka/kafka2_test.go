package kafka

import (
	"fmt"
	"github.com/stretchr/testify/require"
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
