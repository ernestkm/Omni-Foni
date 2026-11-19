// Command sipsmoke drives one SIP call through the edge and asserts the
// expected outcome. Modes: answer (default), deny, exhaust.
package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	target := env("SIP_SMOKE_TARGET", "127.0.0.1:5060")
	dest := env("SIP_SMOKE_DEST", "27821110000")
	from := env("SIP_SMOKE_FROM", "27820000000")
	mode := env("SIP_SMOKE_MODE", "answer")

	raddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		fail("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		fail("listen: %v", err)
	}
	defer conn.Close()

	id := callID()
	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	via := fmt.Sprintf("Via: SIP/2.0/UDP %s;branch=z9hG4bK%s;rport", localAddr(conn), tag)
	invite := "INVITE sip:" + dest + "@edge SIP/2.0\r\n" +
		via + "\r\n" +
		"Max-Forwards: 70\r\n" +
		"From: <" + fromURI(from) + ">;tag=" + tag + "\r\n" +
		"To: <sip:" + dest + ">\r\n" +
		"Call-ID: " + id + "\r\n" +
		"CSeq: 1 INVITE\r\n" +
		"Contact: <sip:sipsmoke@" + localAddr(conn) + ">\r\n" +
		"Content-Length: 0\r\n\r\n"

	buf := make([]byte, 65535)
	var final string
	deadline := time.Now().Add(6 * time.Second)
	if _, err := conn.WriteToUDP([]byte(invite), raddr); err != nil {
		fail("send invite: %v", err)
	}
	nextRetrans := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if time.Now().After(nextRetrans) {
					_, _ = conn.WriteToUDP([]byte(invite), raddr)
					nextRetrans = time.Now().Add(500 * time.Millisecond)
				}
				continue
			}
			fail("read: %v", err)
		}
		final = string(buf[:n])
		if strings.HasPrefix(final, "SIP/2.0 200") || strings.HasPrefix(final, "SIP/2.0 4") || strings.HasPrefix(final, "SIP/2.0 5") || strings.HasPrefix(final, "SIP/2.0 6") {
			break
		}
	}
	if final == "" || !(strings.HasPrefix(final, "SIP/2.0 200") || strings.HasPrefix(final, "SIP/2.0 4") || strings.HasPrefix(final, "SIP/2.0 5") || strings.HasPrefix(final, "SIP/2.0 6")) {
		fail("no final response")
	}
	fmt.Printf("call_id=%s\n", id)
	fmt.Printf("status=%s\n", firstLine(final))
	if reason := reasonHeader(final); reason != "" {
		fmt.Printf("reason=%s\n", reason)
	}

	switch mode {
	case "deny":
		if !strings.HasPrefix(final, "SIP/2.0 403") {
			fail("want 403, got: %s", firstLine(final))
		}
		ack := "ACK sip:" + dest + "@edge SIP/2.0\r\n" + via + "\r\n" +
			"Max-Forwards: 70\r\n" +
			"From: <" + fromURI(from) + ">;tag=" + tag + "\r\n" +
			"To: " + toLine(final) + "\r\n" +
			"Call-ID: " + id + "\r\n" +
			"CSeq: 1 ACK\r\nContent-Length: 0\r\n\r\n"
		_, _ = conn.WriteToUDP([]byte(ack), raddr)
		fmt.Println("ok: sip deny")
		return
	}

	if !strings.HasPrefix(final, "SIP/2.0 200") {
		fail("want 200, got: %s", firstLine(final))
	}
	targetURI := contactURI(final)
	if targetURI == "" {
		targetURI = "sip:" + dest + "@edge"
	}
	ack := "ACK " + targetURI + " SIP/2.0\r\n" + via + "\r\n" +
		"Max-Forwards: 70\r\n" +
		"From: <" + fromURI(from) + ">;tag=" + tag + "\r\n" +
		"To: " + toLine(final) + "\r\n" +
		"Call-ID: " + id + "\r\n" +
		"CSeq: 1 ACK\r\nContent-Length: 0\r\n\r\n"
	_, _ = conn.WriteToUDP([]byte(ack), raddr)

	if mode == "exhaust" {
		// Wait for the edge to hang up after the prepaid balance runs out.
		byeDeadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(byeDeadline) {
			_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				fail("read bye: %v", err)
			}
			msg := string(buf[:n])
			if strings.HasPrefix(msg, "BYE") {
				byeResp := replySimple(msg, "200", "OK")
				_, _ = conn.WriteToUDP([]byte(byeResp), raddr)
				fmt.Println("ok: sip exhaust")
				return
			}
			// ignore stray retransmissions/responses while waiting for teardown
		}
		fail("no teardown bye within 15s")
	}

	time.Sleep(1500 * time.Millisecond)
	bye := "BYE " + targetURI + " SIP/2.0\r\n" + via + "\r\n" +
		"Max-Forwards: 70\r\n" +
		"From: <" + fromURI(from) + ">;tag=" + tag + "\r\n" +
		"To: " + toLine(final) + "\r\n" +
		"Call-ID: " + id + "\r\n" +
		"CSeq: 2 BYE\r\nContent-Length: 0\r\n\r\n"
	_, _ = conn.WriteToUDP([]byte(bye), raddr)

	// Wait for 200 OK to BYE, ignoring stray retransmissions of earlier finals.
	byeDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(byeDeadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			fail("bye read: %v", err)
		}
		msg := string(buf[:n])
		if strings.HasPrefix(msg, "SIP/2.0 200") && strings.Contains(msg, "CSeq: 2 BYE") {
			fmt.Println("ok: sip")
			return
		}
		// 200 OK retransmissions for INVITE (CSeq 1) or other messages: ignore.
	}
	fail("bye not answered 200")
}

func replySimple(req, code, reason string) string {
	lines := strings.Split(req, "\r\n")
	var from, to, callID, cseq string
	var vias []string
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "Via:"):
			vias = append(vias, l)
		case strings.HasPrefix(l, "From:"):
			from = l
		case strings.HasPrefix(l, "To:"):
			to = l
		case strings.HasPrefix(l, "Call-ID:"):
			callID = l
		case strings.HasPrefix(l, "CSeq:"):
			cseq = l
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SIP/2.0 %s %s\r\n", code, reason)
	for _, v := range vias {
		b.WriteString(v + "\r\n")
	}
	b.WriteString(from + "\r\n" + to + "\r\n" + callID + "\r\n" + cseq + "\r\nContent-Length: 0\r\n\r\n")
	return b.String()
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func callID() string {
	if v := os.Getenv("SIP_SMOKE_CALL_ID"); v != "" {
		return v
	}
	return fmt.Sprintf("sipsmoke-%d", time.Now().UnixNano())
}

func fromURI(from string) string {
	if strings.HasPrefix(from, "sip:") || strings.HasPrefix(from, "sips:") {
		if strings.Contains(from, "@") {
			return from
		}
		return from + "@edge"
	}
	return "sip:" + from + "@edge"
}

func reasonHeader(msg string) string {
	for _, line := range strings.Split(msg, "\r\n") {
		if !strings.HasPrefix(line, "Reason:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "Reason:"))
		if i := strings.Index(value, "text="); i >= 0 {
			return strings.Trim(strings.TrimSpace(value[i+len("text="):]), `"`)
		}
	}
	return ""
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func toLine(resp string) string {
	for _, l := range strings.Split(resp, "\r\n") {
		if strings.HasPrefix(l, "To:") {
			return strings.TrimSpace(strings.TrimPrefix(l, "To:"))
		}
	}
	return ""
}

func contactURI(resp string) string {
	for _, l := range strings.Split(resp, "\r\n") {
		if strings.HasPrefix(l, "Contact:") || strings.HasPrefix(l, "m:") {
			v := strings.TrimSpace(strings.TrimPrefix(l, "Contact:"))
			v = strings.TrimSpace(strings.TrimPrefix(v, "m:"))
			if i := strings.Index(v, "<"); i >= 0 {
				if j := strings.Index(v[i+1:], ">"); j >= 0 {
					return v[i+1 : i+1+j]
				}
			}
			// bare URI
			if f := strings.Fields(v); len(f) > 0 {
				return f[0]
			}
		}
	}
	return ""
}

func localAddr(c *net.UDPConn) string {
	port := 5060
	if a, ok := c.LocalAddr().(*net.UDPAddr); ok {
		port = a.Port
	}
	if v := os.Getenv("SIP_SMOKE_CONTACT_HOST"); v != "" {
		return fmt.Sprintf("%s:%d", v, port)
	}
	// Prefer the docker-host gateway address when reachable, so in-dialog
	// requests (BYE on exhaustion) from Kamailio inside the sip network can
	// reach this host probe. Fall back to the route-discovered local IP.
	for _, host := range []string{"172.28.0.1", "172.17.0.1"} {
		ra, err := net.ResolveUDPAddr("udp", host+":5060")
		if err != nil {
			continue
		}
		if d, err := net.DialUDP("udp", nil, ra); err == nil {
			d.Close()
			return fmt.Sprintf("%s:%d", host, port)
		}
	}
	ra, err := net.ResolveUDPAddr("udp", "8.8.8.8:5060")
	if err == nil {
		if d, err := net.DialUDP("udp", nil, ra); err == nil {
			a, ok := d.LocalAddr().(*net.UDPAddr)
			d.Close()
			if ok && !a.IP.IsLoopback() {
				return fmt.Sprintf("%s:%d", a.IP, port)
			}
		}
	}
	fail("cannot determine a usable contact host; set SIP_SMOKE_CONTACT_HOST")
	return ""
}
