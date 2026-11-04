package config

import (
	"os"
	"strings"
)

type Config struct {
	KafkaHost       string
	KafkaTopic      string
	KafkaGroupID    string
	MinioHost       string
	MinioAccessKey  string
	MinioSecretKey  string
	MinioBucket     string
	MinioUseSSL     bool
	HTTPPort        string
}

func Load() *Config {
	kafkaHost := getEnv("KAFKA_HOST", "localhost:9092")
	if servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); servers != "" {
		kafkaHost = servers
	}

	return &Config{
		KafkaHost:      kafkaHost,
		KafkaTopic:     getEnv("KAFKA_TOPIC", "fixia.services.service-requests"),
		KafkaGroupID:   getEnv("KAFKA_GROUP_ID", "ms-intake-lakehouse"),
		MinioHost:      getEnv("MINIO_HOST", "localhost:9000"),
		MinioAccessKey: getEnv("MINIO_ACCESS_KEY", "rustfsadmin"),
		MinioSecretKey: getEnv("MINIO_SECRET_KEY", "rustfsadmin"),
		MinioBucket:    getEnv("MINIO_BUCKET", "lakehouse"),
		MinioUseSSL:    strings.ToLower(getEnv("MINIO_USE_SSL", "false")) == "true",
		HTTPPort:       getEnv("HTTP_PORT", "8080"),
	}
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

