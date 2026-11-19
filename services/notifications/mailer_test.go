package main

import (
	"strings"
	"testing"
)

func TestSMTPMessage(t *testing.T) {
	b := string(smtpMessage("from@example.test", "to@example.test", "hello", "body line\nsecond"))
	for _, want := range []string{
		"From: from@example.test\r\n",
		"To: to@example.test\r\n",
		"Subject: hello\r\n",
		"MIME-Version: 1.0\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"\r\nbody line\nsecond\r\n",
	} {
		if !strings.Contains(b, want) {
			t.Fatalf("message missing %q: %q", want, b)
		}
	}
}
