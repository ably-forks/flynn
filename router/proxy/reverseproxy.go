// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// HTTP reverse proxy handler

package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	log15 "gopkg.in/inconshreveable/log15.v2"
)

const (
	stickyCookie = "_backend"
)

// onExitFlushLoop is a callback set by tests to detect the state of the
// flushLoop() goroutine.
var onExitFlushLoop func()

// Hop-by-hop headers. These are removed when sent to the backend.
// https://tools.ietf.org/html/rfc7230#section-6.1
var (
	hopHeaders = []string{
		"Te", // canonicalized version of "TE"
		"Trailers",
		"Transfer-Encoding",
	}

	serviceUnavailable = []byte("Service Unavailable\n")
	gatewayTimeout     = []byte("Gateway Timeout\n")
	badGateway         = []byte("Bad Gateway\n")
)

type ProxyAttempt struct {
	ForwardedTo string
	Err         error
	Cancelled   bool
	Timeout     bool
	Refused     bool
	Duration    time.Duration
}

type ProxyReport struct {
	Err        error
	Cancelled  bool
	Attempts   []ProxyAttempt
	AllTimeout bool
	AllRefused bool
	Duration   time.Duration
}

func (pr *ProxyReport) LastAttempt() *ProxyAttempt {
	if len(pr.Attempts) == 0 {
		return nil
	}
	return &pr.Attempts[len(pr.Attempts)-1]
}

type ProxyReporter interface {
	SetProxyReport(*ProxyReport)
}

// ReverseProxy is an HTTP Handler that takes an incoming request and
// sends it to another server, proxying the response back to the
// client.
type ReverseProxy struct {
	// The transport used to perform proxy requests.
	transport *transport

	// FlushInterval specifies the flush interval
	// to flush to the client while copying the
	// response body.
	// If zero, no periodic flushing is done.
	FlushInterval time.Duration

	// Logger is the logger for the proxy.
	Logger log15.Logger
}

// NewReverseProxy initializes a new ReverseProxy with a callback to get
// backends, a stickyKey for encrypting sticky session cookies, and a flag
// sticky to enable sticky sessions.
func NewReverseProxy(bf BackendListFunc, stickyKey *[32]byte, sticky bool, l log15.Logger) *ReverseProxy {
	return &ReverseProxy{
		transport: &transport{
			getBackends:       bf,
			stickyCookieKey:   stickyKey,
			useStickySessions: sticky,
		},
		FlushInterval: 10 * time.Millisecond,
		Logger:        l,
	}
}

// ServeHTTP implements http.Handler.
func (p *ReverseProxy) ServeHTTP(ctx context.Context, rw http.ResponseWriter, req *http.Request) {
	transport := p.transport
	if transport == nil {
		panic("router: nil transport for proxy")
	}

	outreq := prepareRequest(req)

	l := p.Logger.New("request_id", req.Header.Get("X-Request-Id"), "client_addr", req.RemoteAddr, "host", req.Host, "path", req.URL.Path, "method", req.Method)

	if isConnectionUpgrade(req.Header) {
		p.serveUpgrade(rw, l, outreq)
		return
	}

	var report *ProxyReport
	resulter, _ := rw.(ProxyReporter)
	setProxyReport := func() func() {
		var set bool
		return func() {
			if set {
				return
			}
			if resulter != nil {
				resulter.SetProxyReport(report)
				set = true
			}
		}
	}()
	defer setProxyReport()
	if resulter != nil {
		report = &ProxyReport{}
	}

	res, err := transport.RoundTrip(ctx, outreq, l, report)
	if resulter != nil {
		report.Err = err
	}
	if err != nil {
		setProxyReport() // Before writing.
		if err == errCancelled {
			return
		}
		if err == errTimeout {
			rw.WriteHeader(http.StatusGatewayTimeout)
			rw.Write(gatewayTimeout)
		} else if err == errRefused {
			rw.WriteHeader(http.StatusBadGateway)
			rw.Write(badGateway)
		} else {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write(serviceUnavailable)
		}
		return
	}
	defer res.Body.Close()

	prepareResponseHeaders(res)
	p.writeResponse(rw, res)
}

// ServeConn takes an inbound conn and proxies it to a backend.
func (p *ReverseProxy) ServeConn(ctx context.Context, dconn net.Conn) {
	transport := p.transport
	if transport == nil {
		panic("router: nil transport for proxy")
	}
	defer dconn.Close()

	l := p.Logger.New("client_addr", dconn.RemoteAddr(), "host_addr", dconn.LocalAddr(), "proxy", "tcp")

	var report *ProxyReport
	resulter, _ := dconn.(ProxyReporter)
	if resulter != nil {
		report = &ProxyReport{}
		resulter.SetProxyReport(report) // Before writing.
	}

	uconn, err := transport.Connect(ctx, l, report)
	if err != nil {
		return
	}
	defer uconn.Close()

	joinConns(l, uconn, dconn)
}

func (p *ReverseProxy) serveUpgrade(rw http.ResponseWriter, l log15.Logger, req *http.Request) {
	transport := p.transport
	if transport == nil {
		panic("router: nil transport for proxy")
	}

	var err error
	var report *ProxyReport
	resulter, _ := rw.(ProxyReporter)
	if resulter != nil {
		report = &ProxyReport{}
		start := time.Now()
		resulter.SetProxyReport(report) // Before writing.
		defer func() {
			report.Err = err
			report.Duration = time.Since(start)
			if a := report.LastAttempt(); a != nil {
				a.Err = err
				a.Duration = report.Duration
			}
		}()
	}

	res, uconn, err := transport.UpgradeHTTP(req, l, report)
	if err != nil {
		if report != nil {
			report.Err = err
		}
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write(serviceUnavailable)
		return
	}
	defer uconn.Close()

	prepareResponseHeaders(res)
	if res.StatusCode != 101 {
		res.Header.Set("Connection", "close")
		p.writeResponse(rw, res)
		return
	}

	dconn, bufrw, err := rw.(http.Hijacker).Hijack()
	if err != nil {
		status, msg := http.StatusServiceUnavailable, serviceUnavailable
		if err == errTimeout {
			status, msg = http.StatusGatewayTimeout, gatewayTimeout
		} else if err == errRefused {
			status, msg = http.StatusBadGateway, badGateway
		}
		l.Error("error hijacking request", "err", err, "status", status)
		rw.WriteHeader(status)
		rw.Write(msg)
		return
	}
	defer dconn.Close()

	err = res.Write(dconn)
	if err != nil {
		l.Error("error proxying response to client", "err", err)
		return
	}

	joinConns(l, uconn, &streamConn{bufrw.Reader, dconn})
}

func prepareResponseHeaders(res *http.Response) {
	// remove global hop-by-hop headers.
	for _, h := range hopHeaders {
		res.Header.Del(h)
	}

	// remove the Upgrade header and headers referenced in the Connection
	// header if HTTP < 1.1 or if Connection header didn't contain "upgrade":
	// https://tools.ietf.org/html/rfc7230#section-6.7
	if !res.ProtoAtLeast(1, 1) || !isConnectionUpgrade(res.Header) {
		res.Header.Del("Upgrade")

		// A proxy or gateway MUST parse a received Connection header field before a
		// message is forwarded and, for each connection-option in this field, remove
		// any header field(s) from the message with the same name as the
		// connection-option, and then remove the Connection header field itself (or
		// replace it with the intermediary's own connection options for the
		// forwarded message): https://tools.ietf.org/html/rfc7230#section-6.1
		tokens := strings.Split(res.Header.Get("Connection"), ",")
		for _, hdr := range tokens {
			res.Header.Del(hdr)
		}
		res.Header.Del("Connection")
	}
}

func (p *ReverseProxy) writeResponse(rw http.ResponseWriter, res *http.Response) {
	copyHeader(rw.Header(), res.Header)

	rw.WriteHeader(res.StatusCode)
	p.copyResponse(rw, res.Body)
}

func isConnectionUpgrade(h http.Header) bool {
	for _, token := range strings.Split(h.Get("Connection"), ",") {
		if v := strings.ToLower(strings.TrimSpace(token)); v == "upgrade" {
			return true
		}
	}
	return false
}

func (p *ReverseProxy) copyResponse(dst io.Writer, src io.Reader) {
	if p.FlushInterval != 0 {
		if wf, ok := dst.(writeFlusher); ok {
			mlw := &maxLatencyWriter{
				dst:     wf,
				latency: p.FlushInterval,
				done:    make(chan bool),
			}
			go mlw.flushLoop()
			defer mlw.stop()
			dst = mlw
		}
	}

	if _, err := io.Copy(dst, src); err != nil {
		p.Logger.Error("error copying from dst to src", "err", err.Error())
	}
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(l log15.Logger, conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil {
			l.Error("error closing write", "err", err.Error())
		}
	} else {
		if err := conn.Close(); err != nil {
			l.Error("error closing", "err", err.Error())
		}
	}
}

func joinConns(l log15.Logger, uconn, dconn net.Conn) {
	done := make(chan struct{})

	go func() {
		if _, err := io.Copy(uconn, dconn); err != nil {
			l.Error("error writing to upstream from downstream", "err", err.Error())
		}
		closeWrite(l, uconn)
		done <- struct{}{}
	}()

	if _, err := io.Copy(dconn, uconn); err != nil {
		l.Error("error writing to downstream from upstream", "err", err.Error())
	}
	closeWrite(l, dconn)
	<-done
}

func prepareRequest(req *http.Request) *http.Request {
	outreq := new(http.Request)
	*outreq = *req // includes shallow copies of maps, but okay

	// Pass the Request-URI verbatim without any modifications
	outreq.URL.Opaque = strings.Split(strings.TrimPrefix(req.RequestURI, req.URL.Scheme+":"), "?")[0]
	outreq.URL.Scheme = "http"
	outreq.Proto = "HTTP/1.1"
	outreq.ProtoMajor = 1
	outreq.ProtoMinor = 1
	outreq.Close = false

	// Remove hop-by-hop headers to the backend.
	outreq.Header = make(http.Header)
	copyHeader(outreq.Header, req.Header)
	for _, h := range hopHeaders {
		outreq.Header.Del(h)
	}

	// remove the Upgrade header and headers referenced in the Connection
	// header if HTTP < 1.1 or if Connection header didn't contain "upgrade":
	// https://tools.ietf.org/html/rfc7230#section-6.7
	if !req.ProtoAtLeast(1, 1) || !isConnectionUpgrade(req.Header) {
		outreq.Header.Del("Upgrade")

		// Especially important is "Connection" because we want a persistent
		// connection, regardless of what the client sent to us.
		outreq.Header.Del("Connection")

		// A proxy or gateway MUST parse a received Connection header field before a
		// message is forwarded and, for each connection-option in this field, remove
		// any header field(s) from the message with the same name as the
		// connection-option, and then remove the Connection header field itself (or
		// replace it with the intermediary's own connection options for the
		// forwarded message): https://tools.ietf.org/html/rfc7230#section-6.1
		tokens := strings.Split(req.Header.Get("Connection"), ",")
		for _, hdr := range tokens {
			outreq.Header.Del(hdr)
		}
	}

	return outreq
}

type writeFlusher interface {
	io.Writer
	http.Flusher
}

type maxLatencyWriter struct {
	dst     writeFlusher
	latency time.Duration

	lk   sync.Mutex // protects Write + Flush
	done chan bool
}

func (m *maxLatencyWriter) Write(p []byte) (int, error) {
	m.lk.Lock()
	defer m.lk.Unlock()
	return m.dst.Write(p)
}

func (m *maxLatencyWriter) flushLoop() {
	t := time.NewTicker(m.latency)
	defer t.Stop()
	for {
		select {
		case <-m.done:
			if onExitFlushLoop != nil {
				onExitFlushLoop()
			}
			return
		case <-t.C:
			m.lk.Lock()
			m.dst.Flush()
			m.lk.Unlock()
		}
	}
}

func (m *maxLatencyWriter) stop() { m.done <- true }
