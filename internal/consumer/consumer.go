package consumer

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/config"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/storage"
	"github.com/Grupo-Fixia/md-intake-lakehouse/internal/validator"
	"github.com/segmentio/kafka-go"
)

type Metrics struct {
	ProcessedTotal   uint64 `json:"processed_total"`
	StoredTotal      uint64 `json:"stored_total"`
	DuplicatesTotal  uint64 `json:"duplicates_total"`
	QuarantinedTotal uint64 `json:"quarantined_total"`
	ErrorsTotal      uint64 `json:"errors_total"`
}

type Consumer struct {
	reader    *kafka.Reader
	storage   *storage.Storage
	validator *validator.Validator
	cfg       *config.Config
	metrics   Metrics
}

func New(cfg *config.Config, store *storage.Storage, val *validator.Validator) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaHost},
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaTopic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Confirmación de offset estrictamente manual (GC-435)
		StartOffset:    kafka.FirstOffset,
	})

	return &Consumer{
		reader:    reader,
		storage:   store,
		validator: val,
		cfg:       cfg,
	}
}

func (c *Consumer) GetMetrics() Metrics {
	return Metrics{
		ProcessedTotal:   atomic.LoadUint64(&c.metrics.ProcessedTotal),
		StoredTotal:      atomic.LoadUint64(&c.metrics.StoredTotal),
		DuplicatesTotal:  atomic.LoadUint64(&c.metrics.DuplicatesTotal),
		QuarantinedTotal: atomic.LoadUint64(&c.metrics.QuarantinedTotal),
		ErrorsTotal:      atomic.LoadUint64(&c.metrics.ErrorsTotal),
	}
}

// Start inicia el bucle infinito de consumo de mensajes de Kafka.
func (c *Consumer) Start(ctx context.Context) error {
	log.Printf("Consumidor Lakehouse iniciado: Topic=%s, GroupID=%s, Broker=%s",
		c.cfg.KafkaTopic, c.cfg.KafkaGroupID, c.cfg.KafkaHost)

	for {
		select {
		case <-ctx.Done():
			log.Println("Deteniendo consumidor Lakehouse...")
			return c.reader.Close()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("Error al obtener mensaje de Kafka: %v. Reintentando en 2s...", err)
			atomic.AddUint64(&c.metrics.ErrorsTotal, 1)
			time.Sleep(2 * time.Second)
			continue
		}

		atomic.AddUint64(&c.metrics.ProcessedTotal, 1)
		c.processMessageWithRetry(ctx, msg)
	}
}

func (c *Consumer) processMessageWithRetry(ctx context.Context, msg kafka.Message) {
	headers := make(map[string]string)
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}

	// 1. Validar sintaxis, correspondencia de clave, cabeceras y esquema
	envelope, err := c.validator.Validate(msg.Value, string(msg.Key), headers)
	if err != nil {
		// Mensaje inválido o incompatible: Enviar a cuarentena y luego confirmar offset
		log.Printf("Mensaje incompatible/inválido en [partición=%d, offset=%d]: %v. Enviando a cuarentena.",
			msg.Partition, msg.Offset, err)

		for {
			errQ := c.storage.StoreQuarantine(ctx, msg.Topic, msg.Partition, msg.Offset, msg.Value, err.Error())
			if errQ == nil {
				atomic.AddUint64(&c.metrics.QuarantinedTotal, 1)
				if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
					log.Printf("Error al confirmar offset tras cuarentena: %v", commitErr)
				}
				return
			}
			log.Printf("Error escribiendo en cuarentena RustFS: %v. Reintentando en 2s...", errQ)
			atomic.AddUint64(&c.metrics.ErrorsTotal, 1)
			time.Sleep(2 * time.Second)
		}
	}

	// 2. Mensaje válido: Guardar en RustFS de forma idempotente
	metadata := map[string]string{
		"topic":       msg.Topic,
		"partition":   fmt.Sprintf("%d", msg.Partition),
		"offset":      fmt.Sprintf("%d", msg.Offset),
		"received-at": time.Now().UTC().Format(time.RFC3339),
	}

	for {
		alreadyExists, errStore := c.storage.StoreEvent(ctx, envelope.Producer, envelope.EventType,
			envelope.SchemaVersion, envelope.OccurredAt, envelope.EventID, msg.Value, metadata)

		if errStore == nil {
			if alreadyExists {
				atomic.AddUint64(&c.metrics.DuplicatesTotal, 1)
				log.Printf("Idempotencia: Evento ya existía en RustFS (eventId=%s, partition=%d, offset=%d)",
					envelope.EventID, msg.Partition, msg.Offset)
			} else {
				atomic.AddUint64(&c.metrics.StoredTotal, 1)
				log.Printf("Evento persistido exitosamente en RustFS: eventId=%s, tipo=%s, seq=%d, partition=%d, offset=%d",
					envelope.EventID, envelope.EventType, envelope.Aggregate.Sequence, msg.Partition, msg.Offset)
			}

			// Confirmar offset únicamente después de asegurar persistencia
			if commitErr := c.reader.CommitMessages(ctx, msg); commitErr != nil {
				log.Printf("Error al confirmar offset de Kafka: %v", commitErr)
			}
			return
		}

		// Fallo en almacenamiento: no confirmar offset y reintentar con backoff
		log.Printf("Fallo al persistir evento en RustFS: %v. Reintentando en 2s...", errStore)
		atomic.AddUint64(&c.metrics.ErrorsTotal, 1)
		time.Sleep(2 * time.Second)
	}
}

