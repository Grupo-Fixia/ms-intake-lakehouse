package validator

import (
	"testing"
)

func TestValidator_ValidEvents(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	validJSON := `{
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

	headers := map[string]string{
		"eventId":       "8c9d0e1f-2a3b-4c4d-9e5f-708192a3b4c1",
		"eventType":     "solicitud.publicada",
		"schemaVersion": "1",
	}

	envelope, err := val.Validate([]byte(validJSON), "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", headers)
	if err != nil {
		t.Fatalf("validation failed unexpectedly: %v", err)
	}

	if envelope.EventID != "8c9d0e1f-2a3b-4c4d-9e5f-708192a3b4c1" {
		t.Errorf("unexpected event ID: %s", envelope.EventID)
	}
	if envelope.EventType != "solicitud.publicada" {
		t.Errorf("unexpected event type: %s", envelope.EventType)
	}
}

func TestValidator_InvalidJSON(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	_, err = val.Validate([]byte(`{not-json}`), "key", nil)
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestValidator_KeyMismatch(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	validJSON := `{
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
			"description": "Fuga",
			"approximateLocation": { "city": "Bogotá", "zone": "Chapinero", "latitude": 4.6533, "longitude": -74.0836 },
			"expectedAt": "2026-10-28T15:00:00Z",
			"publishedAt": "2026-10-26T14:00:00Z",
			"status": "PUBLISHED",
			"photoCount": 0
		}
	}`

	_, err = val.Validate([]byte(validJSON), "different-key", nil)
	if err == nil {
		t.Error("expected error for key mismatch, got nil")
	}
}

func TestValidator_HeaderMismatch(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	validJSON := `{
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
			"description": "Fuga",
			"approximateLocation": { "city": "Bogotá", "zone": "Chapinero", "latitude": 4.6533, "longitude": -74.0836 },
			"expectedAt": "2026-10-28T15:00:00Z",
			"publishedAt": "2026-10-26T14:00:00Z",
			"status": "PUBLISHED",
			"photoCount": 0
		}
	}`

	headers := map[string]string{
		"eventId": "different-id",
	}

	_, err = val.Validate([]byte(validJSON), "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", headers)
	if err == nil {
		t.Error("expected error for header eventId mismatch, got nil")
	}
}

func TestValidator_UnknownType(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	jsonUnknown := `{
		"eventId": "8c9d0e1f-2a3b-4c4d-9e5f-708192a3b4c1",
		"eventType": "tipo.inexistente",
		"schemaVersion": 1,
		"occurredAt": "2026-10-26T14:00:00Z",
		"producer": "ms-services",
		"aggregate": { "type": "ServiceRequest", "id": "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", "sequence": 1 },
		"correlationId": "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7",
		"actor": { "userId": null, "role": "SYSTEM" },
		"data": {}
	}`

	_, err = val.Validate([]byte(jsonUnknown), "6f1c2a3b-4d5e-4f60-8a71-92b3c4d5e6f7", nil)
	if err == nil {
		t.Error("expected error for unknown event type, got nil")
	}
}

