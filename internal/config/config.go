package config

import (
	"os"
	"strings"
)

type Config struct {
	KafkaHost      string
	KafkaTopic     string
	KafkaTopics    []string
	KafkaGroupID   string
	MinioHost      string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket    string
	MinioUseSSL    bool
	HTTPPort       string
}

func Load() *Config {
	kafkaHost := getEnv("KAFKA_HOST", "localhost:9092")
	if servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); servers != "" {
		kafkaHost = servers
	}

	defaultTopics := "fixia.services.service-requests,fixia.payments.events,fixia.users.evaluations"
	rawTopics := os.Getenv("KAFKA_TOPICS")
	if rawTopics == "" {
		rawTopics = getEnv("KAFKA_TOPIC", defaultTopics)
	}
	topics := parseTopics(rawTopics)

	return &Config{
		KafkaHost:      kafkaHost,
		KafkaTopic:     strings.Join(topics, ","),
		KafkaTopics:    topics,
		KafkaGroupID:   getEnv("KAFKA_GROUP_ID", "ms-intake-lakehouse"),
		MinioHost:      getEnv("MINIO_HOST", "localhost:9000"),
		MinioAccessKey: getEnv("MINIO_ACCESS_KEY", "rustfsadmin"),
		MinioSecretKey: getEnv("MINIO_SECRET_KEY", "rustfsadmin"),
		MinioBucket:    getEnv("MINIO_BUCKET", "lakehouse"),
		MinioUseSSL:    strings.ToLower(getEnv("MINIO_USE_SSL", "false")) == "true",
		HTTPPort:       getEnv("HTTP_PORT", "8080"),
	}
}

func parseTopics(raw string) []string {
	var topics []string
	for _, part := range strings.Split(raw, ",") {
		t := strings.TrimSpace(part)
		if t != "" {
			topics = append(topics, t)
		}
	}
	if len(topics) == 0 {
		return []string{"fixia.services.service-requests", "fixia.payments.events", "fixia.users.evaluations"}
	}
	return topics
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

