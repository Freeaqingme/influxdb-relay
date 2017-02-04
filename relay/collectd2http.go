package relay

import (
	"errors"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/collectd"

	"github.com/kimor79/gollectd"
)

type Collectd2HTTP struct {
	addr      string
	name      string
	precision string

	closing int64
	l       *net.UDPConn

	collectdTypes *gollectd.Types
	points        chan models.Point
	backends      []*httpBackend
}

func NewCollectd2Http(config Collectd2HTTPConfig) (Relay, error) {
	u := new(Collectd2HTTP)

	u.name = config.Name
	u.addr = config.Addr

	l, err := net.ListenPacket("udp", u.addr)
	if err != nil {
		return nil, err
	}

	ul, ok := l.(*net.UDPConn)
	if !ok {
		return nil, errors.New("problem listening for UDP")
	}

	if config.ReadBuffer != 0 {
		if err := ul.SetReadBuffer(config.ReadBuffer); err != nil {
			return nil, err
		}
	}

	u.l = ul

	types, err := gollectd.TypesDBFile(config.TypesDBPath)
	if err != nil {
		return nil, err
	}
	u.collectdTypes = &types
	u.points = make(chan models.Point, 1024)

	for i := range config.Outputs {
		backend, err := newHTTPBackend(&config.Outputs[i])
		if err != nil {
			return nil, err
		}

		u.backends = append(u.backends, backend)
	}

	return u, nil
}

func (u *Collectd2HTTP) Name() string {
	if u.name == "" {
		return u.addr
	}
	return u.name
}

func (u *Collectd2HTTP) Run() error {

	// buffer that can hold the largest possible Collectd2HTTP payload
	var buf [65536]byte

	packetQueue := make(chan packet, 1024)
	var wg sync.WaitGroup
	go u.handlePacketQueue(&wg, packetQueue)
	go u.batchPoints()

	log.Printf("Starting Collectd2HTTP relay %q on %v", u.Name(), u.l.LocalAddr())

	for {
		n, remote, err := u.l.ReadFromUDP(buf[:])
		if err != nil {
			if atomic.LoadInt64(&u.closing) == 0 {
				log.Printf("Error reading packet in relay %q from %v: %v", u.name, remote, err)
			} else {
				err = nil
			}
			close(packetQueue)
			wg.Wait()
			return err
		}
		start := time.Now()

		wg.Add(1)

		// copy the data into a buffer and packetQueue it for processing
		b := getUDPBuf()
		b.Grow(n)
		// bytes.Buffer.Write always returns a nil error, and will panic if out of memory
		_, _ = b.Write(buf[:n])
		packetQueue <- packet{start, b, remote}
	}
}

func (u *Collectd2HTTP) batchPoints() {
	batch := make([]string, 0, 5000)
	batchLen := 0
	ticker := time.Tick(500 * time.Millisecond)

	submitBatch := func() {
		log.Printf("Submitting batch of length: %d (%d)\n", len(batch), batchLen)

		payload := []byte(strings.Join(batch, "\n") + "\n")
		wg := &sync.WaitGroup{}
		for _, b := range u.backends {
			wg.Add(1)
			go func(b *httpBackend, wg *sync.WaitGroup) {
				resp, err := b.post(payload, "", "", false)
				if err != nil {
					log.Printf("Problem posting to relay %q backend %q: %v", u.Name(), b.name, err)
				} else {
					if resp.StatusCode/100 == 5 {
						log.Printf("5xx response for relay %q backend %q: %v", u.Name(), b.name, resp.StatusCode)
					}
				}
				wg.Done()
			}(b, wg)
		}
		wg.Wait()
		batch = make([]string, 0, 5000)
		batchLen = 0
	}

	var line string
	for {
		select {
		case p := <-u.points:
			line = p.String()
			batchLen += len(line) + 1
			if batchLen > DefaultBatchSizeKB*KB {
				submitBatch()
				batchLen += len(line) + 1
			}
			batch = append(batch, line)
		case <-ticker:
			if len(batch) > 0 {
				submitBatch()
			}
		}
	}
}

func (u *Collectd2HTTP) handlePacketQueue(wg *sync.WaitGroup, queue <-chan packet) {
	for p := range queue {
		u.handlePacket(&p)
		putUDPBuf(p.data)
		wg.Done()
	}
}

func (u *Collectd2HTTP) handlePacket(udpPacket *packet) {

	packets, err := gollectd.Packets(udpPacket.data.Bytes(), *u.collectdTypes)
	if err != nil {
		log.Printf("Error reading data from %s", udpPacket.from)
	}

	config := collectd.NewConfig()
	collectd := collectd.NewService(config)
	for _, packet := range *packets {
		for _, point := range collectd.UnmarshalCollectd(&packet) {
			u.points <- point
		}
	}

}

func (u *Collectd2HTTP) Stop() error {
	atomic.StoreInt64(&u.closing, 1)
	return u.l.Close()
}
