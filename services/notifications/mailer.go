package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/smtp"
	"time"
)

type SMTPSender struct {
	addr    string
	from    string
	timeout time.Duration
}

func NewSMTPSender(addr, from string, timeout time.Duration) *SMTPSender {
	return &SMTPSender{addr: addr, from: from, timeout: timeout}
}

func smtpMessage(from, to, subject, body string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	fmt.Fprint(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprint(&b, "Content-Type: text/plain; charset=UTF-8\r\n")
	fmt.Fprint(&b, "\r\n")
	fmt.Fprintf(&b, "%s\r\n", body)
	return b.Bytes()
}

func (s *SMTPSender) Send(ctx context.Context, n Notification) error {
	host, _, err := net.SplitHostPort(s.addr)
	if err != nil {
		return err
	}
	d := net.Dialer{Timeout: s.timeout}
	conn, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if s.timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(s.timeout))
	}
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Mail(s.from); err != nil {
		return err
	}
	if err := c.Rcpt(n.Recipient); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(smtpMessage(s.from, n.Recipient, n.Subject, n.Body)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}
