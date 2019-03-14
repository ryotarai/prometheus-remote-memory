package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

const nameLabelKey = "__name__"

type Server struct {
	totalWriteRequests      uint64
	processingWriteRequests int64
	expirationDuration      time.Duration

	samples    sync.Map
	cleanMutex sync.RWMutex
}

func NewServer(expirationDuration time.Duration) (*Server, error) {
	s := &Server{
		totalWriteRequests:      0,
		processingWriteRequests: 0,
		expirationDuration:      expirationDuration,

		samples:    sync.Map{},
		cleanMutex: sync.RWMutex{},
	}
	s.startCleaner()

	return s, nil
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "prometheus_remote_memory_total_write_requests{} %d\n", s.totalWriteRequests)
	fmt.Fprintf(w, "prometheus_remote_memory_processing_write_requests{} %d\n", s.processingWriteRequests)
}

func (s *Server) handleSamples(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	excludeTimestamp := query.Get("excludeTimestamp") != ""

	renderLine := func(name string, sample *prompb.Sample) string {
		if excludeTimestamp {
			return fmt.Sprintf("%s %g\n", name, sample.Value)
		}
		return fmt.Sprintf("%s %g %d\n", name, sample.Value, sample.Timestamp)
	}

	lines := []string{}
	s.samples.Range(func(k, v interface{}) bool {
		name, ok := k.(string)
		if !ok {
			panic("type assertion failed")
		}
		sample, ok := v.(*prompb.Sample)
		if !ok {
			panic("type assertion failed")
		}

		lines = append(lines, renderLine(name, sample))

		return true
	})

	sort.Strings(lines)
	for _, line := range lines {
		fmt.Fprint(w, line)
	}
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&s.totalWriteRequests, 1)
	atomic.AddInt64(&s.processingWriteRequests, 1)
	defer atomic.AddInt64(&s.processingWriteRequests, -1)

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = s.writeTimeseries(req.Timeseries)
	if err != nil {
		log.Printf("Error in writeTimeseries: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) writeTimeseries(tss []*prompb.TimeSeries) error {
	for _, ts := range tss {
		var sample *prompb.Sample
		for _, ss := range ts.Samples {
			if sample == nil || sample.Timestamp < ss.Timestamp {
				sample = ss // latest sample
			}
		}

		labelsCount := len(ts.Labels) // count without __name__
		var name string               // __name__ label
		for _, l := range ts.Labels {
			if l.Name == nameLabelKey {
				name = l.Value
				labelsCount--
			}
		}

		labels := make([]*prompb.Label, labelsCount)
		i := 0
		for _, l := range ts.Labels {
			if l.Name == nameLabelKey {
				continue
			}
			labels[i] = l
			i++
		}
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].Name < labels[j].Name
		})

		labelSlice := make([]string, labelsCount)
		for i, l := range labels {
			if l.Name == nameLabelKey {
				continue
			}
			value := l.Value
			value = strings.Replace(value, "\\", "\\\\", -1)
			value = strings.Replace(value, "\"", "\\\"", -1)
			value = strings.Replace(value, "\n", "\\n", -1)
			labelSlice[i] = fmt.Sprintf("%s=\"%s\"", l.Name, value)
		}
		labelsStr := strings.Join(labelSlice, ",")

		s.cleanMutex.RLock()
		s.samples.Store(fmt.Sprintf("%s{%s}", name, labelsStr), sample)
		s.cleanMutex.RUnlock()
	}

	return nil
}

func (s *Server) startCleaner() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			<-ticker.C
			log.Printf("Cleaning samples...")
			expirationTime := time.Now().Add(s.expirationDuration * -1)
			err := s.cleanBefore(expirationTime)
			if err != nil {
				log.Printf("Cleaning samples failed: %s", err)
			}
			log.Printf("Finished cleaning samples")
		}
	}()
}

func (s *Server) cleanBefore(expirationTime time.Time) error {
	expirationTimeMs := expirationTime.UnixNano() / 1000 / 1000
	var maxDeletedTimestamp int64
	var minDeletedTimestamp int64
	totalCount := 0
	cleanedCount := 0
	s.samples.Range(func(k, v interface{}) bool {
		name, ok := k.(string)
		if !ok {
			panic("type assertion failed")
		}
		sample, ok := v.(*prompb.Sample)
		if !ok {
			panic("type assertion failed")
		}

		totalCount++
		if sample.Timestamp < expirationTimeMs {
			s.cleanMutex.Lock()
			v, ok := s.samples.Load(name)
			if ok {
				sample, ok := v.(*prompb.Sample)
				if !ok {
					panic("type assertion failed")
				}
				if sample.Timestamp < expirationTimeMs { // check again in lock
					if maxDeletedTimestamp == 0 || maxDeletedTimestamp < sample.Timestamp {
						maxDeletedTimestamp = sample.Timestamp
					}
					if minDeletedTimestamp == 0 || minDeletedTimestamp > sample.Timestamp {
						minDeletedTimestamp = sample.Timestamp
					}

					s.samples.Delete(name)
					cleanedCount++
				}
			}
			s.cleanMutex.Unlock()
		}

		return true // keep iteration
	})
	log.Printf("Cleaned %d of %d samples (expirationTimeMs: %d, minDeletedTimestamp: %d, maxDeletedTimestamp: %d)", cleanedCount, totalCount, expirationTimeMs, minDeletedTimestamp, maxDeletedTimestamp)

	return nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/write" {
		s.handleWrite(w, r)
	} else if r.URL.Path == "/samples" {
		s.handleSamples(w, r)
	} else if r.URL.Path == "/metrics" {
		s.handleMetrics(w, r)
	} else {
		http.NotFound(w, r)
	}
}
