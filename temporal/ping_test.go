package temporal

import (
	"testing"
)

func TestPing(t *testing.T) {
	err := Ping()
	if err != nil {
		t.Fatalf("Ping function returned an error: %v", err)
	}
}
