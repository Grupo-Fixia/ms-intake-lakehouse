package validator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
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

	type schemaEntry struct {
		filePath   string
		baseName   string
		canonical  string
		isRootDir  bool
		isEventDoc bool
	}

	var discovered []schemaEntry

	// 1. Recorrer todos los directorios de esquemas incrustados (ms-services, ms-payments, ms-users)
	err := fs.WalkDir(schemas.FS, ".", func(filePath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}

		content, err := fs.ReadFile(schemas.FS, filePath)
		if err != nil {
			return fmt.Errorf("error al leer esquema %s: %w", filePath, err)
		}

		var meta struct {
			ID string `json:"$id"`
		}
		_ = json.Unmarshal(content, &meta)

		canonicalURL := meta.ID
		if canonicalURL == "" {
			canonicalURL = fmt.Sprintf("https://fixia.local/schemas/events/%s", d.Name())
		}

		if err := compiler.AddResource(canonicalURL, bytes.NewReader(content)); err != nil {
			return fmt.Errorf("error al registrar recurso %s: %w", canonicalURL, err)
		}

		isRoot := !strings.Contains(filePath, "/")
		if isRoot {
			if err := compiler.AddResource(d.Name(), bytes.NewReader(content)); err != nil {
				return fmt.Errorf("error al registrar recurso %s: %w", d.Name(), err)
			}
		}

		baseName := path.Base(filePath)
		isEventDoc := baseName != "common.v1.schema.json" && (!isRoot && baseName != "envelope.v1.schema.json" || isRoot)
		discovered = append(discovered, schemaEntry{
			filePath:   filePath,
			baseName:   baseName,
			canonical:  canonicalURL,
			isRootDir:  isRoot,
			isEventDoc: isEventDoc,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error al cargar esquemas incrustados: %w", err)
	}

	// 2. Compilar los esquemas para cada tipo de evento
	compiled := make(map[string]*jsonschema.Schema)
	for _, item := range discovered {
		if !item.isEventDoc {
			continue
		}
		schema, err := compiler.Compile(item.canonical)
		if err != nil {
			return nil, fmt.Errorf("error al compilar esquema %s (%s): %w", item.filePath, item.canonical, err)
		}
		compiled[item.baseName] = schema
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

	// B. Validar correspondencia con la clave del mensaje (debe ser aggregate.id, o data.evaluatedId en evaluacion.registrada)
	if messageKey != "" && envelope.Aggregate.ID != "" && messageKey != envelope.Aggregate.ID {
		evaluatedID, _ := envelope.Data["evaluatedId"].(string)
		if !(envelope.EventType == "evaluacion.registrada" && evaluatedID != "" && messageKey == evaluatedID) {
			return nil, fmt.Errorf("la clave del mensaje Kafka (%s) no coincide con aggregate.id (%s)",
				messageKey, envelope.Aggregate.ID)
		}
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

