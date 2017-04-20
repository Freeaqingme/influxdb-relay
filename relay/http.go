package relay

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert string
	rp   string

	closing int64
	l       net.Listener

	shardsColl *shardCollection
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(cfg HTTPConfig, globalConf Config) (Relay, error) {
	h := new(HTTP)

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	var err error
	h.shardsColl, err = NewShardCollection(cfg.PersistenceName, globalConf)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.l = l

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqPath := r.URL.Path

	if reqPath == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if reqPath == "/status" && (r.Method == "GET" || r.Method == "HEAD") {
		st := make(map[string]map[string]string)

		//for _, b := range h.backends {
		//	st[b.name] = b.poster.getStats()
		//}

		j, err := json.Marshal(st)

		if err != nil {
			log.Printf("error: %s", err)
			jsonResponse(w, http.StatusInternalServerError, "json marshalling failed")
			return
		}

		jsonResponse(w, http.StatusOK, fmt.Sprintf("\"status\": %s", string(j)))
		return
	}

	// We are using a flag to handle the create databases
	isQuery := false
	if reqPath == "/query" {
		isQuery = true
	}

	queryParams := r.URL.Query()

	// Updating data through the http backends has been disabled because it needs to be reimplemented
	// in order to support shards. For now only querying will work.
	if !isQuery {
		jsonResponse(w, http.StatusBadRequest, "Only /query is supported")
		return
	}

	qry, err := influxql.ParseQuery(queryParams["q"][0])
	if err != nil {
		jsonResponse(w, http.StatusBadRequest, "invalid query")
		return
	}

	if len(qry.Statements) != 1 {
		jsonResponse(w, http.StatusBadRequest, "Query must contain a single statement")
		return
	}

	for _, stmt := range qry.Statements {
		var ok bool
		_, ok = stmt.(*influxql.SelectStatement)
		if !ok {
			jsonResponse(w, http.StatusBadRequest, "query not supported, relay only supports CREATE DATABASE queries")
			return
		}
	}

	var shardKey string
	if shardKey = queryParams.Get("shardkey"); shardKey == "" {
		jsonResponse(w, http.StatusBadRequest, "no shard key specified")
		return
	}

	shard := h.shardsColl.GetAssignedShardForShardKey(shardKey)
	if shard == nil {
		jsonResponse(w, http.StatusBadRequest, "Such shard key does not exist")
		return
	}

	bodyBuf := getBuf()
	body := r.Body
	_, err = bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonResponse(w, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonResponse(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	outBuf := getBuf()
	for _, p := range points {
		if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
			break
		}
		if err = outBuf.WriteByte('\n'); err != nil {
			break
		}
	}

	// done with the input points
	putBuf(bodyBuf)
	backend := shard.GetRandomReader()
	if backend == nil {
		jsonResponse(w, http.StatusBadRequest, "No reader backends available in shard")
		return
	}

	queryParams.Add("db", backend.db)

	query := queryParams.Encode()

	outBytes := outBuf.Bytes()
	authHeader := r.Header.Get("Authorization")

	resp, err := backend.post(outBytes, query, authHeader, isQuery)
	w.Header().Add("X-Backend", backend.name)
	w.WriteHeader(resp.StatusCode)
	w.Write(resp.Body)

	putBuf(outBuf)
	/*
		// Don't pass through non create queries
		if reqPath != "/write" {
			fmt.Println(reqPath, isQuery)
			jsonResponse(w, http.StatusNotFound, "invalid write endpoint")
			return
		}

		if r.Method != "POST" {
			w.Header().Set("Allow", "POST")
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
			} else {
				jsonResponse(w, http.StatusMethodNotAllowed, "invalid write method")
			}
			return
		}


		if isQuery {
			// First check if there are multiple queries being passed
			qry, err := influxql.ParseQuery(queryParams["q"][0])
			if err != nil {
				jsonResponse(w, http.StatusBadRequest, "invalid query")
				return
			}
			for _, stmt := range qry.Statements {
				_, ok := stmt.(*influxql.CreateDatabaseStatement)
				if !ok {
					jsonResponse(w, http.StatusBadRequest, "query not supported, relay only supports CREATE DATABASE queries")
					return
				}
			}
		}

		// fail early if we're missing the database
		if !isQuery && queryParams.Get("db") == "" {
			jsonResponse(w, http.StatusBadRequest, "missing parameter: db")
			return
		}

		if queryParams.Get("rp") == "" && h.rp != "" {
			queryParams.Set("rp", h.rp)
		}

		var body = r.Body

		if r.Header.Get("Content-Encoding") == "gzip" {
			b, err := gzip.NewReader(r.Body)
			if err != nil {
				jsonResponse(w, http.StatusBadRequest, "unable to decode gzip body")
			}
			defer b.Close()
			body = b
		}

		bodyBuf := getBuf()
		_, err := bodyBuf.ReadFrom(body)
		if err != nil {
			putBuf(bodyBuf)
			jsonResponse(w, http.StatusInternalServerError, "problem reading request body")
			return
		}

		precision := queryParams.Get("precision")
		points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
		if err != nil {
			putBuf(bodyBuf)
			jsonResponse(w, http.StatusBadRequest, "unable to parse points")
			return
		}

		outBuf := getBuf()
		for _, p := range points {
			if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
				break
			}
			if err = outBuf.WriteByte('\n'); err != nil {
				break
			}
		}

		// done with the input points
		putBuf(bodyBuf)

		if err != nil {
			putBuf(outBuf)
			jsonResponse(w, http.StatusInternalServerError, "problem writing points")
			return
		}

		// normalize query string
		query := queryParams.Encode()

		outBytes := outBuf.Bytes()

		// check for authorization performed via the header
		authHeader := r.Header.Get("Authorization")

		var wg sync.WaitGroup
		wg.Add(len(h.backends))

		var responses = make(chan *responseData, len(h.backends))

		for _, b := range h.backends {
			b := b
			go func() {
				defer wg.Done()
				resp, err := b.post(outBytes, query, authHeader, isQuery)
				if err != nil {
					log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
				} else {
					if resp.StatusCode/100 == 5 {
						log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
					}
					responses <- resp
				}
			}()
		}

		go func() {
			wg.Wait()
			close(responses)
			putBuf(outBuf)
		}()

		var errResponse *responseData

		for resp := range responses {
			switch resp.StatusCode / 100 {
			case 2:
				w.WriteHeader(resp.StatusCode)
				return

			case 4:
				// user error
				resp.Write(w)
				return

			default:
				// hold on to one of the responses to return back to the client
				errResponse = resp
			}
		}

		// no successful writes
		if errResponse == nil {
			// failed to make any valid request...
			jsonResponse(w, http.StatusServiceUnavailable, "unable to write points")
			return
		}

		errResponse.Write(w)*/
}

type responseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

func jsonResponse(w http.ResponseWriter, code int, message string) {
	var data string
	if code/100 != 2 {
		data = fmt.Sprintf("{\"error\":%q}\n", message)
	} else {
		data = fmt.Sprintf("{%s}\n", message)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

type poster interface {
	post([]byte, string, string, bool) (*responseData, error)
	getStats() map[string]string
}

type simplePoster struct {
	client *http.Client
	write  string
	query  string
}

func newSimplePoster(location string, timeout time.Duration, skipTLSVerification bool) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	// Ignore error because location is checked in the config
	qryURL, _ := url.Parse(location)
	qryURL.Path = "/query"
	return &simplePoster{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		write: location,
		query: qryURL.String(),
	}
}

func (s *simplePoster) getStats() map[string]string {
	v := make(map[string]string)
	v["location"] = s.write
	return v
}

func (b *simplePoster) post(buf []byte, query string, auth string, q bool) (*responseData, error) {
	var loc string
	if q {
		loc = b.query
	} else {
		loc = b.write
	}
	req, err := http.NewRequest("POST", loc, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	if query != "" {
		req.URL.RawQuery = query
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	return &responseData{
		ContentType:     resp.Header.Get("Conent-Type"),
		ContentEncoding: resp.Header.Get("Conent-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}

type httpBackend struct {
	poster
	name   string
	writer bool
	reader bool
	db     string
}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p poster = newSimplePoster(cfg.Location, timeout, cfg.SkipTLSVerification)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}

		p = newRetryBuffer(cfg.BufferSizeMB*MB, batch, max, p)
	}

	var writer, reader bool
	switch cfg.Mode {
	case "write":
		writer = true
	case "read":
		return nil, fmt.Errorf("Read-only backends not implemented yet")
		reader = true
	case "readwrite":
	case "":
		writer = true
		reader = true
	default:
		return nil, fmt.Errorf("mode for http backend must be one of: 'write', 'read' or 'readwrite'")
	}

	url, _ := url.Parse(cfg.Location)
	db := url.Query().Get("db")
	if db == "" {
		return nil, fmt.Errorf("Backend location must include 'db' parameter")
	}

	return &httpBackend{
		poster: p,
		name:   cfg.Name,
		reader: reader,
		writer: writer,
		db:     db,
	}, nil
}

var ErrBufferFull = errors.New("retry buffer full")

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}
