package temporal

import (
	"os"
	"testing"
)

func TestPing(t *testing.T) {
	err := Ping()
	if err != nil {
		t.Fatalf("Ping function returned an error: %v", err)
	}

	os.Unsetenv("REDIS_ADDR")

	err = Ping()
	if err == nil {
		t.Fatalf("Ping function should have returned an error when REDIS_ADDR is not set")
	}
}
