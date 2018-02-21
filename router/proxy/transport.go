package proxy

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ably-forks/flynn/pkg/random"
	"golang.org/x/crypto/nacl/secretbox"
	"gopkg.in/inconshreveable/log15.v2"
)

type backendDialer interface {
	DialContext(ctx context.Context, network, addr string) (c net.Conn, err error)
}

var (
	errOther     = proxyError{errors.New("router: some error response from all backends")}
	errRefused   = proxyError{errors.New("router: all backends refused the connection")}
	errTimeout   = proxyError{errors.New("router: timeout from all backends")}
	errCancelled = errors.New("router: backend connection canceled")

	httpTransport = &http.Transport{
		DialContext: customDial,
		// The response header timeout is currently set pretty high because
		// gitreceive doesn't send headers until it is done unpacking the repo,
		// it should be lowered after this is fixed.
		ResponseHeaderTimeout: 10 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second, // unused, but safer to leave default in place
	}

	dialer backendDialer = &net.Dialer{
		Timeout:   1 * time.Second,
		KeepAlive: 30 * time.Second,
	}
)

func tlsDialer(ctx context.Context) *net.Dialer {
	return &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Cancel:    ctx.Done(),
	}
}

type proxyError struct {
	error
}

func IsProxyError(err error) bool {
	_, ok := err.(proxyError)
	return ok
}

// BackendListFunc returns a slice of backend hosts (hostname:port).
type BackendListFunc func(*http.Request) []string

type transport struct {
	getBackends BackendListFunc

	stickyCookieKey   *[32]byte
	useStickySessions bool
}

func (t *transport) getOrderedBackends(stickyBackend string, req *http.Request) []string {
	backends := t.getBackends(req)
	shuffle(backends)

	if stickyBackend != "" {
		swapToFront(backends, stickyBackend)
	}
	return backends
}

func (t *transport) getStickyBackend(req *http.Request) string {
	if t.useStickySessions {
		return getStickyCookieBackend(req, *t.stickyCookieKey)
	}
	return ""
}

func (t *transport) setStickyBackend(res *http.Response, originalStickyBackend string) {
	if !t.useStickySessions {
		return
	}
	if backend := res.Request.URL.Host; backend != originalStickyBackend {
		setStickyCookieBackend(res, backend, *t.stickyCookieKey)
	}
}

func (t *transport) RoundTrip(ctx context.Context, req *http.Request, l log15.Logger, report *ProxyReport) (*http.Response, error) {
	// http.Transport closes the request body on a failed dial, issue #875
	req.Body = &fakeCloseReadCloser{req.Body}
	//for Ably router, we close the body in the router after a possible retry
	//attempt; we do not want Flynn to close it
	//defer req.Body.(*fakeCloseReadCloser).RealClose()

	req = req.WithContext(ctx)

	stickyBackend := t.getStickyBackend(req)
	backends := t.getOrderedBackends(stickyBackend, req)

	timeouts := 0
	refused := 0

	start := time.Now()
	if report != nil {
		defer func() {
			report.Duration = time.Since(start)
		}()
	}
	for i, backend := range backends {
		req.URL.Host = backend
		req.Header.Set("X-Forwarded-To", backend)
		start := time.Now()
		res, err := httpTransport.RoundTrip(req)
		attempt := ProxyAttempt{
			ForwardedTo: backend,
			Err:         err,
			Duration:    time.Since(start),
		}
		if err == nil {
			t.setStickyBackend(res, stickyBackend)
		} else if strings.Contains(err.Error(), "canceled") {
			attempt.Cancelled = true
		} else if strings.Contains(err.Error(), "timeout") {
			attempt.Timeout = true
			timeouts++
		} else if strings.Contains(err.Error(), "connection refused") {
			attempt.Refused = true
			refused++
		}
		if report != nil {
			report.Attempts = append(report.Attempts, attempt)
		}
		if err == nil {
			return res, nil
		} else if attempt.Cancelled {
			if report != nil {
				report.Cancelled = true
			}
			return nil, errCancelled
		}

		l.Error("retriable dial error", "backend", backend, "err", err, "attempt", i, "duration", time.Since(start))
	}

	allTimeout := timeouts == len(backends)
	allRefused := refused == len(backends)

	if report != nil {
		report.AllTimeout = allTimeout
		report.AllRefused = allRefused
	}

	status, err := 503, errOther
	if allTimeout {
		status, err = 504, errTimeout
	} else if allRefused {
		status, err = 502, errRefused
	}
	l.Error("request failed", "status", status, "num_backends", len(backends), "duration", time.Since(start))
	return nil, err
}

func (t *transport) Connect(ctx context.Context, l log15.Logger, report *ProxyReport) (net.Conn, error) {
	backends := t.getOrderedBackends("", nil)
	conn, _, err := dialTCP(ctx, l, backends, report)
	if err != nil {
		if report != nil {
			report.Err = err
		}
		l.Error("connection failed", "num_backends", len(backends))
	}
	return conn, err
}

func (t *transport) UpgradeHTTP(req *http.Request, l log15.Logger, report *ProxyReport) (*http.Response, net.Conn, error) {
	stickyBackend := t.getStickyBackend(req)
	backends := t.getOrderedBackends(stickyBackend, req)
	upconn, addr, err := dialTCP(req.Context(), l, backends, report)
	if err != nil {
		l.Error("dial failed", "err", err, "num_backends", len(backends))
		return nil, nil, err
	}
	conn := &streamConn{bufio.NewReader(closeWhenCanceled(req.Context(), upconn)), upconn}
	req.URL.Host = addr
	req.Header.Set("X-Forwarded-To", addr)
	if err := req.Write(conn); err != nil {
		conn.Close()
		l.Error("error writing request", "err", err, "backend", addr)
		return nil, nil, err
	}
	res, err := http.ReadResponse(conn.Reader, req)
	if err != nil {
		conn.Close()
		if err == context.Canceled {
			err = errCancelled
			if report != nil {
				report.Cancelled = true
			}
		} else {
			l.Error("error reading response", "err", err, "backend", addr)
		}
		return nil, nil, err
	}
	t.setStickyBackend(res, stickyBackend)
	return res, conn, nil
}

func dialTCP(ctx context.Context, l log15.Logger, addrs []string, report *ProxyReport) (net.Conn, string, error) {
	donec := ctx.Done()

	timeouts := 0
	refused := 0

	for i, addr := range addrs {
		start := time.Now()
		attempt := ProxyAttempt{
			ForwardedTo: addr,
		}

		var conn net.Conn
		var err error
		select {
		case <-donec:
			attempt.Duration = time.Since(start)
			attempt.Cancelled = true
			attempt.Err = ctx.Err()
		default:
			if strings.HasPrefix(addr, "https://") {
				conn, err = tls.DialWithDialer(tlsDialer(ctx), "tcp", strings.TrimPrefix(addr, "https://"), nil)
			} else {
				conn, err = dialer.DialContext(ctx, "tcp", addr)
			}
			attempt.Err = err
			if err != nil {
				attempt.Duration = time.Since(start)
				if strings.Contains(err.Error(), "canceled") {
					attempt.Cancelled = true
				} else if strings.Contains(err.Error(), "timeout") {
					attempt.Timeout = true
					timeouts++
				} else if strings.Contains(err.Error(), "connection refused") {
					attempt.Refused = true
					refused++
				}
			}
		}
		if report != nil {
			report.Attempts = append(report.Attempts, attempt)
		}
		if attempt.Cancelled {
			if report != nil {
				report.Cancelled = true
			}
			return nil, "", errCancelled
		} else if err == nil {
			return conn, addr, nil
		}

		l.Error("retriable dial error", "backend", addr, "err", err, "attempt", i)
	}

	allTimeout := timeouts == len(addrs)
	allRefused := refused == len(addrs)

	if report != nil {
		report.AllTimeout = allTimeout
		report.AllRefused = allRefused
	}

	err := errOther
	if allTimeout {
		err = errTimeout
	} else if allRefused {
		err = errRefused
	}
	return nil, "", err
}

func customDial(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, dialErr{err}
	}
	return conn, nil
}

type dialErr struct {
	error
}

type fakeCloseReadCloser struct {
	io.ReadCloser
}

func (w *fakeCloseReadCloser) Close() error {
	return nil
}

func (w *fakeCloseReadCloser) RealClose() error {
	if w.ReadCloser == nil {
		return nil
	}
	return w.ReadCloser.Close()
}

func shuffle(s []string) {
	for i := len(s) - 1; i > 0; i-- {
		j := random.Math.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func swapToFront(ss []string, s string) {
	for i := range ss {
		if ss[i] == s {
			ss[0], ss[i] = ss[i], ss[0]
			return
		}
	}
}

func getStickyCookieBackend(req *http.Request, cookieKey [32]byte) string {
	cookie, err := req.Cookie(stickyCookie)
	if err != nil {
		return ""
	}

	data, err := base64.StdEncoding.DecodeString(cookie.Value)
	if err != nil {
		return ""
	}
	return string(decrypt(data, cookieKey))
}

func setStickyCookieBackend(res *http.Response, backend string, cookieKey [32]byte) {
	cookie := http.Cookie{
		Name:  stickyCookie,
		Value: base64.StdEncoding.EncodeToString(encrypt([]byte(backend), cookieKey)),
		Path:  "/",
	}
	res.Header.Add("Set-Cookie", cookie.String())
}

func encrypt(data []byte, key [32]byte) []byte {
	var nonce [24]byte
	_, err := io.ReadFull(rand.Reader, nonce[:])
	if err != nil {
		panic(err)
	}

	out := make([]byte, len(nonce), len(nonce)+len(data)+secretbox.Overhead)
	copy(out, nonce[:])
	return secretbox.Seal(out, data, &nonce, &key)
}

func decrypt(data []byte, key [32]byte) []byte {
	var nonce [24]byte
	if len(data) < len(nonce) {
		return nil
	}
	copy(nonce[:], data)
	res, ok := secretbox.Open(nil, data[len(nonce):], &nonce, &key)
	if !ok {
		return nil
	}
	return res
}

// cancelError wraps an error in a goroutine safe way, so that access to the
// error doesn't cause a deadlock.
type cancelError struct {
	mtx sync.RWMutex
	err error
}

// SetErr sets an error with locking semantics
// If there already exists an error, this will over-write the existing error
// with the new one.
func (c *cancelError) SetErr(err error) {
	c.mtx.Lock()
	c.err = err
	c.mtx.Unlock()
}

// Err retrives an error with locking semantics.
// If no error exists, err will be nil
func (c *cancelError) Err() (err error) {
	c.mtx.RLock()
	err = c.err
	c.mtx.RUnlock()
	return
}

func closeWhenCanceled(ctx context.Context, r io.ReadCloser) io.Reader {
	var (
		canceledErr = new(cancelError)

		readReqs = make(chan closeWhenCanceledRequest)
		doneCh   = make(chan struct{})
	)

	go func() {
		// Stop when the context is Done or a Read operation fails.
		select {
		case <-ctx.Done():
			canceledErr.SetErr(ctx.Err())
			r.Close() // This will cause the Read in the other goroutine to stop.
		case <-doneCh:
		}
	}()

	go func() {
		// Reader goroutine; serves requests from the returned Reader's Read
		// method.
		for readReq := range readReqs {
			i, err := r.Read(readReq.b)
			if e := canceledErr.Err(); e != nil {
				err = e
			}
			readReq.response <- struct {
				i   int
				err error
			}{i, err}

			if err != nil {
				close(doneCh)
				return
			}
		}
	}()

	return &closeWhenCanceledReader{readReqs: readReqs}
}

type closeWhenCanceledRequest struct {
	b        []byte
	response chan<- struct {
		i   int
		err error
	}
}

type closeWhenCanceledReader struct {
	readReqs chan<- closeWhenCanceledRequest
	lastErr  error
}

func (r *closeWhenCanceledReader) Read(b []byte) (int, error) {
	if err := r.lastErr; err != nil {
		// After an error, the server goroutine is dead. Just return it.
		return 0, err
	}

	respCh := make(chan struct {
		i   int
		err error
	})
	r.readReqs <- closeWhenCanceledRequest{b, respCh}
	resp := <-respCh
	r.lastErr = resp.err
	return resp.i, resp.err
}
