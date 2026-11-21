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

func TestValidator_PaymentAndUserEvents(t *testing.T) {
	val, err := New()
	if err != nil {
		t.Fatalf("error initializing validator: %v", err)
	}

	paymentJSON := `{
		"eventId": "e6161fce-64bd-46b1-8d49-818dce5390b8",
		"eventType": "pago.procesado",
		"schemaVersion": 1,
		"occurredAt": "2026-11-02T20:02:55.536580Z",
		"producer": "ms-payments",
		"aggregate": { "type": "Payment", "id": "05717e69-811e-42db-8343-39fb9a16c99c", "sequence": 2 },
		"correlationId": "11111111-1111-4111-8111-111111111111",
		"actor": { "userId": "22222222-2222-4222-8222-222222222222", "role": "CLIENT" },
		"data": {
			"paymentId": "05717e69-811e-42db-8343-39fb9a16c99c",
			"serviceRequestId": "11111111-1111-4111-8111-111111111111",
			"contractId": "44444444-4444-4444-8444-444444444444",
			"clientId": "22222222-2222-4222-8222-222222222222",
			"providerId": "33333333-3333-4333-8333-333333333333",
			"amount": { "amount": "120750.00", "currency": "COP" },
			"breakdown": {
				"price": { "amount": "105000.00", "currency": "COP" },
				"commission": { "amount": "15750.00", "currency": "COP" },
				"rate": "0.1500",
				"ruleId": "00000000-0000-0000-0000-000000000015"
			},
			"status": "Processing"
		}
	}`

	envPay, err := val.Validate([]byte(paymentJSON), "05717e69-811e-42db-8343-39fb9a16c99c", map[string]string{
		"eventId":       "e6161fce-64bd-46b1-8d49-818dce5390b8",
		"eventType":     "pago.procesado",
		"schemaVersion": "1",
	})
	if err != nil {
		t.Fatalf("expected valid payment event, got error: %v", err)
	}
	if envPay.Producer != "ms-payments" {
		t.Errorf("expected producer ms-payments, got %s", envPay.Producer)
	}

	evalJSON := `{
		"eventId": "f39dcca9-4808-4f3c-8550-1e0c245354ae",
		"eventType": "evaluacion.registrada",
		"schemaVersion": 1,
		"occurredAt": "2026-11-05T00:56:58.123456Z",
		"producer": "ms-users",
		"aggregate": { "type": "Evaluation", "id": "51b56d3a-4c57-4c0b-8af2-86cc2c7baf69", "sequence": 1 },
		"correlationId": "7cbb353a-ca95-4d11-b396-d70d057fbf3f",
		"actor": { "userId": "03fe8fe1-441d-43c5-bc5b-91268a104f36", "role": "CLIENT" },
		"data": {
			"evaluationId": "51b56d3a-4c57-4c0b-8af2-86cc2c7baf69",
			"serviceRequestId": "7cbb353a-ca95-4d11-b396-d70d057fbf3f",
			"direction": "CLIENT_TO_PROVIDER",
			"evaluatorId": "03fe8fe1-441d-43c5-bc5b-91268a104f36",
			"evaluatedId": "92f16a59-ed52-4bb6-a513-0155c19bd172",
			"rating": 5,
			"source": "CONFIRMED_CLOSURE",
			"serviceClosedAt": "2026-11-05T00:56:50.000000Z"
		}
	}`

	// En ms-users, la clave del mensaje Kafka es evaluatedId
	envEval, err := val.Validate([]byte(evalJSON), "92f16a59-ed52-4bb6-a513-0155c19bd172", map[string]string{
		"eventId":       "f39dcca9-4808-4f3c-8550-1e0c245354ae",
		"eventType":     "evaluacion.registrada",
		"schemaVersion": "1",
	})
	if err != nil {
		t.Fatalf("expected valid evaluation event with evaluatedId key, got error: %v", err)
	}
	if envEval.Producer != "ms-users" {
		t.Errorf("expected producer ms-users, got %s", envEval.Producer)
	}
}

