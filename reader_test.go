package sarama

import (
	"strconv"
	"testing"
	"time"
)

func TestLoadData(t *testing.T) {
	config := NewConfig()
	config.Producer.Flush.Frequency = 3000 * time.Millisecond
	config.Producer.Compression = CompressionLZ4
	producer, err := NewAsyncProducer([]string{"localhost:9092"}, config)

	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	defer producer.Close()
	for i := 0; i < 10; i++ {
		now := time.Now()
		msg := &ProducerMessage{
			Topic: "test-topic-1",
			Key:   StringEncoder("data-" + strconv.Itoa(i)),
			Value: StringEncoder(now.String()),
		}

		producer.Input() <- msg
	}

	// Wait for the messages to be sent
	time.Sleep(5 * time.Second)
}

func TestTransaction(t *testing.T) {
	config := NewConfig()
	config.Producer.Flush.Frequency = 3000 * time.Millisecond
	config.Producer.Compression = CompressionLZ4
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = "test-transaction-id-1"
	producer, err := NewAsyncProducer([]string{"localhost:9092"}, config)

	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	defer producer.Close()

	// Start a transaction
	err = producer.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 10; i++ {
		now := time.Now()
		msg := &ProducerMessage{
			Topic: "test-topic-1",
			Key:   StringEncoder("txn-" + strconv.Itoa(i)),
			Value: StringEncoder(now.String()),
		}

		producer.Input() <- msg
	}

	for i := range 15 {
		t.Logf("sleeping %d(th) seconds", i)
		time.Sleep(time.Second)
	}

	for i := 0; i < 10; i++ {
		now := time.Now()
		msg := &ProducerMessage{
			Topic: "test-topic-2",
			Key:   StringEncoder("txn-" + strconv.Itoa(i)),
			Value: StringEncoder(now.String()),
		}

		producer.Input() <- msg
	}

	time.Sleep(3 * time.Second)

	err = producer.CommitTxn()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	time.Sleep(3 * time.Second)
}

func TestDefaultReader_Read(t *testing.T) {
	config := NewConfig()
	reader, err := NewReader([]string{"localhost:9092"}, config)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}

	defer func(reader Reader) {
		err := reader.Close()
		if err != nil {
			t.Fatalf("Failed to close reader: %v", err)
		}
	}(reader)

	tp, err := reader.TopicMetadata()
	if err != nil {
		t.Fatalf("Failed to get topic metadata: %v", err)
	}

	if len(tp) == 0 {
		t.Fatalf("Expected topic metadata, but got none")
	}

	for _, i := range tp {
		t.Logf("Topic Metadata: %s", i)
	}

	resp, err := reader.Read("test-topic-1", 0, 122)
	if err != nil {
		t.Fatalf("Failed to read message: %v", err)
	}

	if resp == nil {
		t.Fatalf("Expected a response, but got nil")
	}

	for topic, partitionBlocks := range resp.Blocks {
		for partition, block := range partitionBlocks {
			if block == nil {
				t.Fatalf("Expected a block for topic %s, partition %d, but got nil", topic, partition)
			}
			for _, r := range block.RecordsSet {
				off, e := r.recordsOffset()
				if e != nil {
					t.Fatalf("Failed to get records offset: %v", e)
				}
				if off == nil {
					t.Fatalf("Expected a records offset, but got nil")
				}
				t.Logf("RecordBatch first offset: %d", *off)
				for _, record := range r.RecordBatch.Records {
					t.Logf("Read record: delta: %v value: %s", record.OffsetDelta, string(record.Value))
				}
			}
		}
	}
}
