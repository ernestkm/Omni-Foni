package main

import "testing"

func TestEnvInt(t *testing.T) {
	t.Setenv("FRAUD_TEST_INT", "7")
	if got := envInt("FRAUD_TEST_INT", 3); got != 7 {
		t.Fatalf("envInt = %d", got)
	}
	t.Setenv("FRAUD_TEST_INT", "")
	if got := envInt("FRAUD_TEST_INT", 3); got != 3 {
		t.Fatalf("default = %d", got)
	}
}
