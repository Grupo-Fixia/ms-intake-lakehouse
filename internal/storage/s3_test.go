package storage

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/config"
	"github.com/minio/minio-go/v7"
)

type mockObjectClient struct {
	bucketExists bool
	bucketErr    error
	makeErr      error
	statInfo     minio.ObjectInfo
	statErr      error
	putInfo      minio.UploadInfo
	putErr       error
	lastKey      string
}

func (m *mockObjectClient) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	return m.bucketExists, m.bucketErr
}

func (m *mockObjectClient) MakeBucket(ctx context.Context, bucketName string, opts minio.MakeBucketOptions) error {
	return m.makeErr
}

func (m *mockObjectClient) StatObject(ctx context.Context, bucketName, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return m.statInfo, m.statErr
}

func (m *mockObjectClient) PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	m.lastKey = objectName
	return m.putInfo, m.putErr
}

func TestNewStorage(t *testing.T) {
	cfg := &config.Config{
		MinioHost:      "http://localhost:9000",
		MinioAccessKey: "rustfsadmin",
		MinioSecretKey: "rustfsadmin",
		MinioBucket:    "lakehouse",
		MinioUseSSL:    false,
	}
	s, err := New(cfg)
	if err != nil || s == nil {
		t.Fatalf("expected valid Storage, got err=%v", err)
	}

	badCfg := &config.Config{
		MinioHost: "http://bad host with spaces:9000",
	}
	if _, err := New(badCfg); err == nil {
		t.Errorf("expected error for invalid host")
	}
}

func TestEnsureBucketExists(t *testing.T) {
	ctx := context.Background()

	// 1. Bucket does not exist -> creates it
	m := &mockObjectClient{bucketExists: false}
	s := &Storage{client: m, bucket: "lakehouse"}
	if err := s.EnsureBucketExists(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2. BucketExists error
	m.bucketErr = errors.New("network error")
	if err := s.EnsureBucketExists(ctx); err == nil {
		t.Errorf("expected error when BucketExists fails")
	}

	// 3. MakeBucket error
	m.bucketErr = nil
	m.makeErr = errors.New("cannot create")
	if err := s.EnsureBucketExists(ctx); err == nil {
		t.Errorf("expected error when MakeBucket fails")
	}
}

func TestStoreEvent(t *testing.T) {
	ctx := context.Background()

	// 1. Already exists (StatObject returns nil error)
	m := &mockObjectClient{statErr: nil}
	s := &Storage{client: m, bucket: "lakehouse"}
	exists, err := s.StoreEvent(ctx, "ms-services", "solicitud.publicada", 1, "2026-10-26T14:00:00Z", "id-1", []byte(`{}`), map[string]string{"topic": "t"})
	if err != nil || !exists {
		t.Fatalf("expected exists=true, err=nil; got exists=%v, err=%v", exists, err)
	}

	// 2. Does not exist (404) -> PutObject succeeds
	m.statErr = errors.New("404 NoSuchKey: object does not exist")
	exists, err = s.StoreEvent(ctx, "ms-services", "solicitud.publicada", 1, "2026-10-26T14:00:00Z", "id-1", []byte(`{}`), map[string]string{"topic": "t"})
	if err != nil || exists {
		t.Fatalf("expected exists=false, err=nil; got exists=%v, err=%v", exists, err)
	}

	// 3. StatObject unexpected error
	m.statErr = errors.New("connection refused")
	if _, err := s.StoreEvent(ctx, "ms-services", "solicitud.publicada", 1, "2026-10-26T14:00:00Z", "id-1", []byte(`{}`), nil); err == nil {
		t.Errorf("expected error on non-404 StatObject error")
	}

	// 4. PutObject failure
	m.statErr = errors.New("does not exist")
	m.putErr = errors.New("disk full")
	if _, err := s.StoreEvent(ctx, "ms-services", "solicitud.publicada", 1, "2026-10-26T14:00:00Z", "id-1", []byte(`{}`), nil); err == nil {
		t.Errorf("expected error on PutObject failure")
	}
}

func TestStoreQuarantine(t *testing.T) {
	ctx := context.Background()
	m := &mockObjectClient{}
	s := &Storage{client: m, bucket: "lakehouse"}

	if err := s.StoreQuarantine(ctx, "fixia.services.service-requests", 0, 42, []byte(`{bad}`), "invalid schema"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.putErr = errors.New("write error")
	if err := s.StoreQuarantine(ctx, "fixia.services.service-requests", 0, 42, []byte(`{bad}`), "invalid schema"); err == nil {
		t.Errorf("expected error when PutObject fails in StoreQuarantine")
	}
}

func TestExtractDate(t *testing.T) {
	date := extractDate("2026-10-26T14:00:00Z")
	if date != "2026-10-26" {
		t.Errorf("expected 2026-10-26, got %s", date)
	}

	shortDate := extractDate("short")
	if len(shortDate) != 10 {
		t.Errorf("expected 10 chars date, got %s", shortDate)
	}
}

func TestSanitizeKey(t *testing.T) {
	res := sanitizeKey("fixia/services/requests")
	if res != "fixia-services-requests" {
		t.Errorf("expected fixia-services-requests, got %s", res)
	}
}

func TestTruncate(t *testing.T) {
	res := truncate("hello world", 5)
	if res != "hello" {
		t.Errorf("expected hello, got %s", res)
	}

	short := truncate("abc", 10)
	if short != "abc" {
		t.Errorf("expected abc, got %s", short)
	}
}
