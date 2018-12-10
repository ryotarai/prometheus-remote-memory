package main

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"

	"github.com/stretchr/testify/assert"
)

func TestClean(t *testing.T) {
	server, err := NewServer(time.Minute)
	assert.NoError(t, err)

	ts1 := time.Unix(1, 0)
	ts2 := time.Unix(2, 0)
	ts3 := time.Unix(3, 0)

	tss := []*prompb.TimeSeries{
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "a"},
			},
			Samples: []*prompb.Sample{
				{Timestamp: ts1.UnixNano(), Value: 10.0},
			},
		},
		{
			Labels: []*prompb.Label{
				{Name: "__name__", Value: "b"},
			},
			Samples: []*prompb.Sample{
				{Timestamp: ts3.UnixNano(), Value: 10.0},
			},
		},
	}

	err = server.writeTimeseries(tss)
	assert.NoError(t, err)

	err = server.cleanBefore(ts2)
	assert.NoError(t, err)

	count := 0
	server.samples.Range(func(k, v interface{}) bool {
		// name, ok := k.(string)
		// if !ok {
		// 	panic("type assertion failed")
		// }
		// sample, ok := v.(*prompb.Sample)
		// if !ok {
		// 	panic("type assertion failed")
		// }

		count++
		return true
	})

	assert.Equal(t, 1, count)

	// testServer := httptest.NewServer(server)
	// http.
	// 	testServer.URL
}
