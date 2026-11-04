package storage

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storage struct {
	client *minio.Client
	bucket string
}

func New(cfg *config.Config) (*Storage, error) {
	endpoint := cfg.MinioHost
	// Limpiar prefijo http:// o https:// si viene en la variable
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioAccessKey, cfg.MinioSecretKey, ""),
		Secure: cfg.MinioUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("error al inicializar cliente MinIO/RustFS: %w", err)
	}

	return &Storage{
		client: client,
		bucket: cfg.MinioBucket,
	}, nil
}

// EnsureBucketExists verifica que el bucket del lakehouse exista, o lo crea si no está presente.
func (s *Storage) EnsureBucketExists(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("error al verificar existencia de bucket %s: %w", s.bucket, err)
	}
	if !exists {
		err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("error al crear bucket %s: %w", s.bucket, err)
		}
		log.Printf("Bucket %s creado exitosamente en RustFS", s.bucket)
	}
	return nil
}

// StoreEvent guarda un evento válido en RustFS de forma idempotente (GC-435).
// Retorna alreadyExists = true si el objeto con ese eventId ya existía en el almacenamiento.
func (s *Storage) StoreEvent(ctx context.Context, producer, eventType string, schemaVersion int, occurredAt string, eventID string, payload []byte, metadata map[string]string) (bool, error) {
	dateFolder := extractDate(occurredAt)
	objectKey := fmt.Sprintf("events/producer=%s/type=%s/v=%d/date=%s/%s.json",
		producer, eventType, schemaVersion, dateFolder, eventID)

	// Regla de idempotencia (Task 408 / GC-435): La primera copia gana
	_, err := s.client.StatObject(ctx, s.bucket, objectKey, minio.StatObjectOptions{})
	if err == nil {
		// El objeto ya existe: no se sobreescribe y se considera exitoso
		return true, nil
	}

	errResp := minio.ToErrorResponse(err)
	if errResp.Code != "NoSuchKey" && !strings.Contains(err.Error(), "does not exist") && !strings.Contains(err.Error(), "404") {
		return false, fmt.Errorf("error al verificar existencia previa de %s: %w", objectKey, err)
	}

	// Almacenar el sobre tal como llegó
	userMeta := make(map[string]string)
	for k, v := range metadata {
		userMeta[k] = v
	}

	opts := minio.PutObjectOptions{
		ContentType:  "application/json",
		UserMetadata: userMeta,
	}

	_, err = s.client.PutObject(ctx, s.bucket, objectKey, bytes.NewReader(payload), int64(len(payload)), opts)
	if err != nil {
		return false, fmt.Errorf("error al escribir objeto %s en RustFS: %w", objectKey, err)
	}

	return false, nil
}

// StoreQuarantine guarda un mensaje inválido o incompatible en la ruta de cuarentena (GC-435).
func (s *Storage) StoreQuarantine(ctx context.Context, topic string, partition int, offset int64, payload []byte, reason string) error {
	today := time.Now().UTC().Format("2006-01-02")
	objectKey := fmt.Sprintf("quarantine/date=%s/%s-%d-%d.json", today, sanitizeKey(topic), partition, offset)

	userMeta := map[string]string{
		"quarantine-reason": truncate(reason, 200),
		"topic":             topic,
		"partition":         fmt.Sprintf("%d", partition),
		"offset":            fmt.Sprintf("%d", offset),
		"received-at":       time.Now().UTC().Format(time.RFC3339),
	}

	opts := minio.PutObjectOptions{
		ContentType:  "application/json",
		UserMetadata: userMeta,
	}

	_, err := s.client.PutObject(ctx, s.bucket, objectKey, bytes.NewReader(payload), int64(len(payload)), opts)
	if err != nil {
		return fmt.Errorf("error al escribir en cuarentena %s: %w", objectKey, err)
	}

	return nil
}

func extractDate(occurredAt string) string {
	if len(occurredAt) >= 10 {
		return occurredAt[:10]
	}
	return time.Now().UTC().Format("2006-01-02")
}

func sanitizeKey(name string) string {
	return strings.ReplaceAll(name, "/", "-")
}

func truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen]
	}
	return s
}

