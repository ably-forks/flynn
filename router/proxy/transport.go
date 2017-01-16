package proxy

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/ably-forks/flynn/pkg/random"
	"golang.org/x/crypto/nacl/secretbox"
	"golang.org/x/net/context"
	"gopkg.in/inconshreveable/log15.v2"
)

type backendDialer interface {
	Dial(network, addr string) (c net.Conn, err error)
}

var (
	errOther    = proxyError{errors.New("router: some error response from all backends")}
	errRefused  = proxyError{errors.New("router: all backends refused the connection")}
	errTimeout  = proxyError{errors.New("router: timeout from all backends")}
	errCanceled = errors.New("router: backend connection canceled")

	httpTransport = &http.Transport{
		Dial: customDial,
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

	tlsDialer = &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
)

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

func (t *transport) RoundTrip(ctx context.Context, req *http.Request, l log15.Logger) (*http.Response, error) {
	// http.Transport closes the request body on a failed dial, issue #875
	req.Body = &fakeCloseReadCloser{req.Body}
	defer req.Body.(*fakeCloseReadCloser).RealClose()

	// hook up CloseNotify to cancel the request
	req.Cancel = ctx.Done()

	stickyBackend := t.getStickyBackend(req)
	backends := t.getOrderedBackends(stickyBackend, req)
	allTimeout := len(backends) > 0
	allRefused := len(backends) > 0
	for i, backend := range backends {
		req.URL.Host = backend
		res, err := httpTransport.RoundTrip(req)
		if err == nil {
			t.setStickyBackend(res, stickyBackend)
			return res, nil
		}
		if allTimeout && !strings.Contains(err.Error(), "timeout") {
			allTimeout = false
		}
		if allRefused && !strings.Contains(err.Error(), "connection refused") {
			allRefused = false
		}
		l.Error("retriable dial error", "backend", backend, "err", err, "attempt", i)
	}
	status, err := 503, errOther
	if allTimeout {
		status, err = 504, errTimeout
	} else if allRefused {
		status, err = 502, errRefused
	}
	l.Error("request failed", "status", status, "num_backends", len(backends))
	return nil, err
}

func (t *transport) Connect(ctx context.Context, l log15.Logger) (net.Conn, error) {
	backends := t.getOrderedBackends("", nil)
	conn, _, err := dialTCP(ctx, l, backends)
	if err != nil {
		l.Error("connection failed", "num_backends", len(backends))
	}
	return conn, err
}

func (t *transport) UpgradeHTTP(req *http.Request, l log15.Logger) (*http.Response, net.Conn, error) {
	stickyBackend := t.getStickyBackend(req)
	backends := t.getOrderedBackends(stickyBackend, req)
	upconn, addr, err := dialTCP(context.Background(), l, backends)
	if err != nil {
		status := http.StatusServiceUnavailable
		if err == errTimeout {
			status = http.StatusGatewayTimeout
		}
		l.Error("dial failed", "status", status, "num_backends", len(backends))
		return nil, nil, err
	}
	conn := &streamConn{bufio.NewReader(upconn), upconn}
	req.URL.Host = addr

	if err := req.Write(conn); err != nil {
		conn.Close()
		l.Error("error writing request", "err", err, "backend", addr)
		return nil, nil, err
	}
	res, err := http.ReadResponse(conn.Reader, req)
	if err != nil {
		conn.Close()
		l.Error("error reading response", "err", err, "backend", addr)
		return nil, nil, err
	}
	t.setStickyBackend(res, stickyBackend)
	return res, conn, nil
}

func dialTCP(ctx context.Context, l log15.Logger, addrs []string) (net.Conn, string, error) {
	donec := ctx.Done()

	allTimeout := len(addrs) > 0
	allRefused := len(addrs) > 0
	for i, addr := range addrs {
		select {
		case <-donec:
			return nil, "", errCanceled
		default:
		}

		var conn net.Conn
		var err error
		if strings.HasPrefix(addr, "https://") {
			conn, err = tls.DialWithDialer(tlsDialer, "tcp", strings.TrimPrefix(addr, "https://"), nil)
		} else {
			conn, err = dialer.Dial("tcp", addr)
		}
		if err == nil {
			return conn, addr, nil
		}
		if allTimeout && !strings.Contains(err.Error(), "timeout") {
			allTimeout = false
		}
		if allRefused && !strings.Contains(err.Error(), "connection refused") {
			allRefused = false
		}
		l.Error("retriable dial error", "backend", addr, "err", err, "attempt", i)
	}
	err := errOther
	if allTimeout {
		err = errTimeout
	} else if allRefused {
		err = errRefused
	}
	return nil, "", err
}

func customDial(network, addr string) (net.Conn, error) {
	conn, err := dialer.Dial(network, addr)
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
