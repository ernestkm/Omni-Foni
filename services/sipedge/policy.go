package main

import (
	"net/http"
	"strings"
)

// parseAccounts parses "ip=account,ip=account" into a lookup table.
func parseAccounts(s string) map[string]string {
	m := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		ip, acct, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		ip, acct = strings.TrimSpace(ip), strings.TrimSpace(acct)
		if ip != "" && acct != "" {
			m[ip] = acct
		}
	}
	return m
}

// sipCode maps a switching deny reason to an HTTP status the SIP edge turns
// into a SIP reply: policy denials are 403, control-plane outages are 503.
func sipCode(reason string) int {
	switch reason {
	case "ocs_unavailable", "routing_unavailable", "sbc_unavailable",
		"fraud_unavailable", "no_rate", "internal_error":
		return http.StatusServiceUnavailable
	default:
		return http.StatusForbidden
	}
}

// userPart extracts the user portion of a SIP URI, e.g. "sip:2782111@h" -> "2782111".
func userPart(uri string) string {
	uri = strings.TrimPrefix(uri, "sip:")
	uri = strings.TrimPrefix(uri, "sips:")
	if i := strings.IndexByte(uri, '@'); i >= 0 {
		uri = uri[:i]
	}
	if i := strings.IndexByte(uri, ';'); i >= 0 {
		uri = uri[:i]
	}
	return uri
}
