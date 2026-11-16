package main

import (
	"fmt"
	"math/big"
)

// RateCost mirrors the OCS money rule: ROUND(seconds * rate_per_sec) in
// integer minor units, half away from zero.
func RateCost(seconds int64, rate string) (int64, error) {
	r, ok := new(big.Rat).SetString(rate)
	if !ok {
		return 0, fmt.Errorf("invalid rate %q", rate)
	}
	r.Mul(r, new(big.Rat).SetInt64(seconds))

	num, den := r.Num(), r.Denom() // den > 0
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	rem.Abs(rem)
	rem.Lsh(rem, 1) // 2*|rem| >= den means a half or more
	if rem.Cmp(den) >= 0 {
		if r.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if !q.IsInt64() {
		return 0, fmt.Errorf("rate cost overflow for %d * %s", seconds, rate)
	}
	return q.Int64(), nil
}
