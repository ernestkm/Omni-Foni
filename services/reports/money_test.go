package main

import "testing"

func TestRateCost(t *testing.T) {
	cases := []struct {
		seconds int64
		rate    string
		want    int64
	}{
		{0, "0.012345", 0},
		{1, "0.5", 1},       // 0.5 rounds half away from zero
		{3, "0.5", 2},       // 1.5 -> 2
		{-3, "0.5", -2},     // -1.5 -> -2
		{60, "0.012345", 1}, // 0.7407 -> 1
		{61, "0.012345", 1}, // 0.753045 -> 1
		{1, "0.0000004", 0}, // 0.0000004 -> 0
		{1000000, "12.345678", 12345678},
	}
	for _, c := range cases {
		got, err := RateCost(c.seconds, c.rate)
		if err != nil {
			t.Fatalf("RateCost(%d,%q): %v", c.seconds, c.rate, err)
		}
		if got != c.want {
			t.Errorf("RateCost(%d,%q) = %d, want %d", c.seconds, c.rate, got, c.want)
		}
	}
}

func TestRateCostRejectsBadRate(t *testing.T) {
	for _, rate := range []string{"", "abc", "1.2.3"} {
		if _, err := RateCost(1, rate); err == nil {
			t.Errorf("RateCost(1,%q) = nil error, want error", rate)
		}
	}
}
