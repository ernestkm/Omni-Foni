package scoring

import "time"

const RuleVersion = "v1"

type Config struct {
	AlertThreshold   int
	SoftAccountCalls int
	SoftPrefixCalls  int
}

type Signals struct {
	Destination      string
	CLI              string
	Start            time.Time
	RiskPrefixWeight int
	FirstSeenPrefix  bool
	FirstSeenCLI     bool
	AccountCalls     int
	PrefixCalls      int
}

func DestinationPrefix(destination string) string {
	if len(destination) <= 4 {
		return destination
	}
	return destination[:4]
}

func Score(cfg Config, s Signals) (int, []string) {
	score := 0
	var reasons []string
	if s.RiskPrefixWeight > 0 {
		score += s.RiskPrefixWeight
		reasons = append(reasons, "risk_prefix")
	}
	if s.FirstSeenPrefix {
		score += 20
		reasons = append(reasons, "first_seen_prefix")
	}
	if cfg.SoftAccountCalls > 0 && s.AccountCalls >= cfg.SoftAccountCalls ||
		cfg.SoftPrefixCalls > 0 && s.PrefixCalls >= cfg.SoftPrefixCalls {
		score += 20
		reasons = append(reasons, "velocity")
	}
	if s.FirstSeenCLI {
		score += 15
		reasons = append(reasons, "first_seen_cli")
	}
	if s.Start.UTC().Hour() < 6 {
		score += 10
		reasons = append(reasons, "odd_hour")
	}
	if score > 100 {
		score = 100
	}
	return score, reasons
}

func Level(score int) string {
	switch {
	case score >= 80:
		return "high"
	case score >= 40:
		return "medium"
	default:
		return "low"
	}
}
