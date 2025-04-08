package sarama

import (
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

type Reader interface {
	Close() error
	Config() *Config
	TopicMetadata() ([]*TopicMetadata, error)
	// Read fetches a batch of messages from the given topic and partition, starting at the given offset.
	// It returns a FetchResponse containing the messages and any errors encountered during the fetch.
	Read(topic string, partition int32, offset int64) (*FetchResponse, error)
}

type defaultReader struct {
	conf           *Config
	client         Client
	metricRegistry metrics.Registry
	lock           sync.Mutex
}

func NewReader(addrs []string, config *Config) (Reader, error) {
	if config == nil {
		config = NewConfig()
	}
	client, err := NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	return &defaultReader{
		conf:           config,
		client:         client,
		metricRegistry: metrics.NewRegistry(),
	}, nil
}

func (r *defaultReader) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.client != nil {
		if err := r.client.Close(); err != nil {
			return err
		}
		r.client = nil
	}

	return nil
}

func (r *defaultReader) Config() *Config {
	return r.conf
}

func (r *defaultReader) makeFetchRequest() (*FetchRequest, error) {
	tmp := &FetchRequest{
		MinBytes:    r.conf.Consumer.Fetch.Min,
		MaxWaitTime: int32(r.conf.Consumer.MaxWaitTime / time.Millisecond),
	}
	// Version 1 is the same as version 0.
	if r.conf.Version.IsAtLeast(V0_9_0_0) {
		tmp.Version = 1
	}
	// Starting in Version 2, the requestor must be able to handle Kafka Log
	// Message format version 1.
	if r.conf.Version.IsAtLeast(V0_10_0_0) {
		tmp.Version = 2
	}
	// Version 3 adds MaxBytes.  Starting in version 3, the partition ordering in
	// the tmp is now relevant.  Partitions will be processed in the order
	// they appear in the tmp.
	if r.conf.Version.IsAtLeast(V0_10_1_0) {
		tmp.Version = 3
		tmp.MaxBytes = MaxResponseSize
	}
	// Version 4 adds IsolationLevel.  Starting in version 4, the reqestor must be
	// able to handle Kafka log message format version 2.
	// Version 5 adds LogStartOffset to indicate the earliest available offset of
	// partition data that can be consumed.
	if r.conf.Version.IsAtLeast(V0_11_0_0) {
		tmp.Version = 5
		tmp.Isolation = r.conf.Consumer.IsolationLevel
	}
	// Version 6 is the same as version 5.
	if r.conf.Version.IsAtLeast(V1_0_0_0) {
		tmp.Version = 6
	}
	// Version 7 adds incremental fetch tmp support.
	if r.conf.Version.IsAtLeast(V1_1_0_0) {
		tmp.Version = 7
		// We do not currently implement KIP-227 FetchSessions. Setting the id to 0
		// and the epoch to -1 tells the broker not to generate as session ID we're going
		// to just ignore anyway.
		tmp.SessionID = 0
		tmp.SessionEpoch = -1
	}
	// Version 8 is the same as version 7.
	if r.conf.Version.IsAtLeast(V2_0_0_0) {
		tmp.Version = 8
	}
	// Version 9 adds CurrentLeaderEpoch, as described in KIP-320.
	// Version 10 indicates that we can use the ZStd compression algorithm, as
	// described in KIP-110.
	if r.conf.Version.IsAtLeast(V2_1_0_0) {
		tmp.Version = 10
	}
	// Version 11 adds RackID for KIP-392 fetch from closest replica
	if r.conf.Version.IsAtLeast(V2_3_0_0) {
		tmp.Version = 11
		tmp.RackID = r.conf.RackID
	}

	return tmp, nil
}

func (r *defaultReader) Read(topic string, partition int32, offset int64) (*FetchResponse, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	leader, epoch, err := r.client.LeaderAndEpoch(topic, partition)

	if err != nil {
		return nil, err
	}

	req, _ := r.makeFetchRequest()

	req.AddBlock(topic, partition, offset, r.conf.Consumer.Fetch.Max, epoch)

	return leader.Fetch(req)
}

func (r *defaultReader) TopicMetadata() ([]*TopicMetadata, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	metadata, err := r.client.TopicMetadata()
	if err != nil {
		return nil, err
	}

	return metadata, nil
}
