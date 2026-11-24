package config

import (
	"os"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	os.Clearenv()
	cfg := Load()

	if cfg.KafkaHost != "localhost:9092" {
		t.Errorf("expected localhost:9092, got %s", cfg.KafkaHost)
	}
	if len(cfg.KafkaTopics) != 3 {
		t.Errorf("expected 3 default topics, got %d", len(cfg.KafkaTopics))
	}
	if cfg.KafkaGroupID != "ms-intake-lakehouse" {
		t.Errorf("expected ms-intake-lakehouse, got %s", cfg.KafkaGroupID)
	}
	if cfg.MinioBucket != "lakehouse" {
		t.Errorf("expected lakehouse, got %s", cfg.MinioBucket)
	}
	if cfg.MinioUseSSL {
		t.Errorf("expected MinioUseSSL false")
	}
	if cfg.HTTPPort != "8080" {
		t.Errorf("expected 8080, got %s", cfg.HTTPPort)
	}
}

func TestLoadCustomEnv(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-0:9092")
	t.Setenv("KAFKA_TOPICS", "topic.a, topic.b , ")
	t.Setenv("KAFKA_GROUP_ID", "custom-group")
	t.Setenv("MINIO_HOST", "rustfs:9000")
	t.Setenv("MINIO_ACCESS_KEY", "ak")
	t.Setenv("MINIO_SECRET_KEY", "sk")
	t.Setenv("MINIO_BUCKET", "custom-bucket")
	t.Setenv("MINIO_USE_SSL", "true")
	t.Setenv("HTTP_PORT", "9090")

	cfg := Load()
	if cfg.KafkaHost != "kafka-0:9092" {
		t.Errorf("expected kafka-0:9092, got %s", cfg.KafkaHost)
	}
	if len(cfg.KafkaTopics) != 2 || cfg.KafkaTopics[0] != "topic.a" || cfg.KafkaTopics[1] != "topic.b" {
		t.Errorf("unexpected topics: %v", cfg.KafkaTopics)
	}
	if !cfg.MinioUseSSL {
		t.Errorf("expected MinioUseSSL true")
	}
	if cfg.HTTPPort != "9090" {
		t.Errorf("expected 9090, got %s", cfg.HTTPPort)
	}
}

func TestParseTopicsEmptyFallback(t *testing.T) {
	topics := parseTopics(" , , ")
	if len(topics) != 3 {
		t.Errorf("expected 3 fallback topics, got %d", len(topics))
	}
}
