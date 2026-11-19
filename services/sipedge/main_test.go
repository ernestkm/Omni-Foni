package main

import (
	"testing"
	"time"
)

func TestReapInterval(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    time.Duration
		wantOn  bool
		wantErr bool
	}{
		{name: "unset", raw: "", want: time.Second, wantOn: true},
		{name: "explicit disable", raw: "0", want: 0, wantOn: false},
		{name: "zero duration", raw: "0s", want: 0, wantOn: false},
		{name: "negative duration", raw: "-1s", want: 0, wantOn: false},
		{name: "explicit duration", raw: "2s", want: 2 * time.Second, wantOn: true},
		{name: "invalid", raw: "nope", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, on, err := reapInterval(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error for %q", tc.raw)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want || on != tc.wantOn {
				t.Fatalf("%q: want (%v, %t), got (%v, %t)", tc.raw, tc.want, tc.wantOn, got, on)
			}
		})
	}
}
