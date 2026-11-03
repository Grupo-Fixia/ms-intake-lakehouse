package storage

import (
	"testing"
)

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

