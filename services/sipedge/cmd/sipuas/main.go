// Command sipuas is a scripted SIP user-agent server used by the SIP edge
// smoke and end-to-end tests. It answers INVITE with 200 OK + SDP and BYE
// with 200 OK. It is deliberately minimal: no registration, no re-INVITE.
package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	addr := os.Getenv("SIPUAS_ADDR")
	if addr == "" {
		addr = ":5060"
	}
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	slog.Info("sipuas up", "addr", addr)
	buf := make([]byte, 65535)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			continue
		}
		msg := string(buf[:n])
		switch {
		case strings.HasPrefix(msg, "INVITE"):
			slog.Info("invite", "from", from.String())
			_ = send(pc, from, reply(msg, "200", "OK", sdpBody(from)))
		case strings.HasPrefix(msg, "BYE"):
			slog.Info("bye", "from", from.String())
			_ = send(pc, from, reply(msg, "200", "OK", ""))
		case strings.HasPrefix(msg, "CANCEL"):
			_ = send(pc, from, reply(msg, "200", "OK", ""))
		}
	}
}

func send(pc net.PacketConn, to net.Addr, msg string) error {
	_, err := pc.WriteTo([]byte(msg), to)
	return err
}

func reply(req, code, reason, body string) string {
	lines := strings.Split(req, "\r\n")
	var from, to, callID, cseq string
	var vias []string
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "Via:"):
			vias = append(vias, l)
		case strings.HasPrefix(l, "From:"):
			from = strings.TrimSpace(strings.TrimPrefix(l, "From:"))
		case strings.HasPrefix(l, "To:"):
			to = strings.TrimSpace(strings.TrimPrefix(l, "To:"))
		case strings.HasPrefix(l, "Call-ID:"):
			callID = strings.TrimSpace(strings.TrimPrefix(l, "Call-ID:"))
		case strings.HasPrefix(l, "CSeq:"):
			cseq = strings.TrimSpace(strings.TrimPrefix(l, "CSeq:"))
		}
	}
	toTag := ";tag=sipuas-" + fmt.Sprint(time.Now().UnixNano())
	if !strings.Contains(to, ";tag=") {
		to += toTag
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SIP/2.0 %s %s\r\n", code, reason)
	for _, v := range vias {
		b.WriteString(v + "\r\n")
	}
	b.WriteString("From: " + from + "\r\n")
	b.WriteString("To: " + to + "\r\n")
	b.WriteString("Call-ID: " + callID + "\r\n")
	b.WriteString("CSeq: " + cseq + "\r\n")
	b.WriteString("Contact: <sip:sipuas@sipuas:5060>\r\n")
	b.WriteString("Server: sipuas\r\n")
	if body != "" {
		b.WriteString("Content-Type: application/sdp\r\n")
		fmt.Fprintf(&b, "Content-Length: %d\r\n", len(body))
	}
	b.WriteString("\r\n")
	b.WriteString(body)
	return b.String()
}

func sdpBody(from net.Addr) string {
	host, _, _ := net.SplitHostPort(from.String())
	return "v=0\r\no=sipuas 0 0 IN IP4 " + host + "\r\ns=-\r\nc=IN IP4 " + host +
		"\r\nt=0 0\r\nm=audio 40000 RTP/AVP 0\r\n"
}
