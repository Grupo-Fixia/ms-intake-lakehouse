package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"strings"

	"github.com/Grupo-Fixia/md-intake-lakehouse/schemas"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// EventEnvelope representa el sobre estándar de eventos de Fixia (envelope.v1).
type EventEnvelope struct {
	EventID       string                 `json:"eventId"`
	EventType     string                 `json:"eventType"`
	SchemaVersion int                    `json:"schemaVersion"`
	OccurredAt    string                 `json:"occurredAt"`
	Producer      string                 `json:"producer"`
	Aggregate     AggregateInfo          `json:"aggregate"`
	CorrelationID string                 `json:"correlationId"`
	Actor         ActorInfo              `json:"actor"`
	Data          map[string]interface{} `json:"data"`
}

type AggregateInfo struct {
	Type     string `json:"type"`
	ID       string `json:"id"`
	Sequence int64  `json:"sequence"`
}

type ActorInfo struct {
	UserID *string `json:"userId"`
	Role   string  `json:"role"`
}

// Validator compila y valida los eventos contra los esquemas JSON de Fixia (GC-436).
type Validator struct {
	compiledSchemas map[string]*jsonschema.Schema
}

func New() (*Validator, error) {
	compiler := jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft2020

	entries, err := fs.ReadDir(schemas.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("error al leer esquemas incrustados: %w", err)
	}

	// 1. Registrar todos los esquemas en el compilador usando su URL canónica y nombre de archivo
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			content, err := fs.ReadFile(schemas.FS, entry.Name())
			if err != nil {
				return nil, fmt.Errorf("error al leer esquema %s: %w", entry.Name(), err)
			}

			canonicalURL := fmt.Sprintf("https://fixia.local/schemas/events/%s", entry.Name())
			if err := compiler.AddResource(canonicalURL, bytes.NewReader(content)); err != nil {
				return nil, fmt.Errorf("error al registrar recurso %s: %w", canonicalURL, err)
			}
			if err := compiler.AddResource(entry.Name(), bytes.NewReader(content)); err != nil {
				return nil, fmt.Errorf("error al registrar recurso %s: %w", entry.Name(), err)
			}
		}
	}

	// 2. Compilar los esquemas para cada tipo de evento
	compiled := make(map[string]*jsonschema.Schema)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") && entry.Name() != "common.v1.schema.json" {
			canonicalURL := fmt.Sprintf("https://fixia.local/schemas/events/%s", entry.Name())
			schema, err := compiler.Compile(canonicalURL)
			if err != nil {
				return nil, fmt.Errorf("error al compilar esquema %s: %w", entry.Name(), err)
			}
			compiled[entry.Name()] = schema
		}
	}

	return &Validator{
		compiledSchemas: compiled,
	}, nil
}

// Validate valida la sintaxis, integridad de cabeceras/clave y el esquema del evento.
func (v *Validator) Validate(payload []byte, messageKey string, headers map[string]string) (*EventEnvelope, error) {
	// A. Deserializar el sobre JSON
	var envelope EventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("JSON malformado o inválido: %w", err)
	}

	// B. Validar correspondencia con la clave del mensaje (debe ser aggregate.id)
	if messageKey != "" && envelope.Aggregate.ID != "" && messageKey != envelope.Aggregate.ID {
		return nil, fmt.Errorf("la clave del mensaje Kafka (%s) no coincide con aggregate.id (%s)",
			messageKey, envelope.Aggregate.ID)
	}

	// C. Validar cabeceras mínimas si están presentes
	if hEventID, ok := headers["eventId"]; ok && hEventID != envelope.EventID {
		return nil, fmt.Errorf("cabecera eventId (%s) no coincide con el cuerpo (%s)", hEventID, envelope.EventID)
	}
	if hEventType, ok := headers["eventType"]; ok && hEventType != envelope.EventType {
		return nil, fmt.Errorf("cabecera eventType (%s) no coincide con el cuerpo (%s)", hEventType, envelope.EventType)
	}
	if hVersion, ok := headers["schemaVersion"]; ok {
		expectedVer := fmt.Sprintf("%d", envelope.SchemaVersion)
		if hVersion != expectedVer {
			return nil, fmt.Errorf("cabecera schemaVersion (%s) no coincide con el cuerpo (%s)", hVersion, expectedVer)
		}
	}

	// D. Buscar el esquema correspondiente al tipo y versión
	schemaName := fmt.Sprintf("%s.v%d.schema.json", envelope.EventType, envelope.SchemaVersion)
	schema, exists := v.compiledSchemas[schemaName]
	if !exists {
		return nil, fmt.Errorf("tipo de evento o versión no soportada: %s (esquema %s no encontrado)",
			envelope.EventType, schemaName)
	}

	// E. Validar el documento completo contra el JSON Schema
	var untyped interface{}
	if err := json.Unmarshal(payload, &untyped); err != nil {
		return nil, fmt.Errorf("error al procesar estructura JSON: %w", err)
	}

	if err := schema.Validate(untyped); err != nil {
		return nil, fmt.Errorf("violación de esquema %s: %w", schemaName, err)
	}

	return &envelope, nil
}

