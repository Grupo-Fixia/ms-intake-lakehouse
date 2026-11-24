package consumer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/config"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/validator"
	"github.com/segmentio/kafka-go"
)

type mockReader struct {
	messages   []kafka.Message
	idx        int
	fetchErr   error
	commitErr  error
	closed     bool
	cancelFunc context.CancelFunc
}

func (m *mockReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	if m.fetchErr != nil {
		err := m.fetchErr
		m.fetchErr = nil
		if m.cancelFunc != nil {
			m.cancelFunc()
		}
		return kafka.Message{}, err
	}
	if m.idx < len(m.messages) {
		msg := m.messages[m.idx]
		m.idx++
		return msg, nil
	}
	if m.cancelFunc != nil {
		m.cancelFunc()
	}
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (m *mockReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return m.commitErr
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

type mockEventStorage struct {
	alreadyExists bool
	storeErr      error
	storeErrCount int
	quarantineErr error
	qErrCount     int
}

func (m *mockEventStorage) StoreEvent(ctx context.Context, producer, eventType string, schemaVersion int, occurredAt string, eventID string, payload []byte, metadata map[string]string) (bool, error) {
	if m.storeErrCount > 0 {
		m.storeErrCount--
		return false, m.storeErr
	}
	return m.alreadyExists, nil
}

func (m *mockEventStorage) StoreQuarantine(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error {
	if m.qErrCount > 0 {
		m.qErrCount--
		return m.quarantineErr
	}
	return nil
}

const validPublishedEvent = `{
	"eventId": "8c9d0e1f-2a3b-4c4d-9e5f-708192a3b4c1",
	"eventType": "solicitud.publicada",
	"schemaVersion": 1,
	"occurredAt": "2026-10-26T14:00:00Z",
	"producer": "ms-services",
	"aggregate": { "type": "ServiceRequest", "id": "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", "sequence": 1 },
	"correlationId": "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7",
	"actor": { "userId": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d", "role": "CLIENT" },
	"data": {
		"serviceRequestId": "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7",
		"clientId": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
		"category": "PLUMBING",
		"description": "Fuga de agua debajo del lavamanos",
		"approximateLocation": { "city": "Bogotá", "zone": "Chapinero", "latitude": 4.6533, "longitude": -74.0836 },
		"expectedAt": "2026-10-28T15:00:00Z",
		"publishedAt": "2026-10-26T14:00:00Z",
		"status": "PUBLISHED",
		"photoCount": 2
	}
}`

func TestNewAndStartConsumer(t *testing.T) {
	val, err := validator.New()
	if err != nil {
		t.Fatalf("validator init error: %v", err)
	}

	cfg := &config.Config{
		KafkaHost:    "127.0.0.1:1",
		KafkaTopic:   "fixia.services.service-requests",
		KafkaGroupID: "test-group",
	}

	c := New(cfg, nil, val)
	if c == nil || c.reader == nil {
		t.Fatalf("expected non-nil consumer")
	}
	_ = c.reader.Close()

	// Replace reader and storage with mocks to test Start, processMessageWithRetry, and GetMetrics
	ctx, cancel := context.WithCancel(context.Background())
	mr := &mockReader{
		cancelFunc: cancel,
		messages: []kafka.Message{
			{
				Topic:     "fixia.services.service-requests",
				Partition: 0,
				Offset:    1,
				Key:       []byte("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7"),
				Value:     []byte(validPublishedEvent),
				Headers: []kafka.Header{
					{Key: "eventId", Value: []byte("8c9d0e1f-2a3b-4c4d-9e5f-708192a3b4c1")},
				},
			},
			{
				Topic:     "fixia.services.service-requests",
				Partition: 0,
				Offset:    2,
				Key:       []byte("bad-key"),
				Value:     []byte(`{invalid-json`),
			},
		},
	}
	ms := &mockEventStorage{
		storeErr:      errors.New("temporary s3 failure"),
		storeErrCount: 1,
		quarantineErr: errors.New("temporary quarantine failure"),
		qErrCount:     1,
	}

	c.reader = mr
	c.storage = ms
	c.retryDelay = 1 * time.Millisecond

	if err := c.Start(ctx); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}

	if !mr.closed {
		t.Errorf("expected reader to be closed on shutdown")
	}

	m := c.GetMetrics()
	if m.ProcessedTotal != 2 || m.StoredTotal != 1 || m.QuarantinedTotal != 1 || m.ErrorsTotal != 2 {
		t.Errorf("unexpected metrics: %+v", m)
	}
}

func TestProcessDuplicateAndFetchError(t *testing.T) {
	val, _ := validator.New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen err: %v", err)
	}
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
	}()
	defer ln.Close()

	cfg := &config.Config{
		KafkaHost:    ln.Addr().String(),
		KafkaTopics:  []string{"fixia.services.service-requests"},
		KafkaGroupID: "test-group",
	}

	ctx, cancel := context.WithCancel(context.Background())
	mr := &mockReader{
		fetchErr:   errors.New("broker transient error"),
		commitErr:  errors.New("commit failed"),
		cancelFunc: cancel,
	}
	ms := &mockEventStorage{
		alreadyExists: true,
	}

	c := &Consumer{
		reader:     mr,
		storage:    ms,
		validator:  val,
		cfg:        cfg,
		retryDelay: 1 * time.Millisecond,
	}

	c.processMessageWithRetry(ctx, kafka.Message{
		Topic: "fixia.services.service-requests",
		Key:   []byte("6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7"),
		Value: []byte(validPublishedEvent),
	})

	_ = c.Start(ctx)

	m := c.GetMetrics()
	if m.DuplicatesTotal != 1 {
		t.Errorf("expected DuplicatesTotal=1, got %d", m.DuplicatesTotal)
	}
	if m.ErrorsTotal == 0 {
		t.Errorf("expected ErrorsTotal > 0")
	}
}
