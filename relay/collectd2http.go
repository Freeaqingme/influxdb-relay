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
	points        map[string]chan models.Point
	shardsColl    *shardCollection
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

	u.shardsColl, err = NewShardCollection(config.Shards, config.ShardAssignmentStore)
	if err != nil {
		return nil, err
	}

	u.points = make(map[string]chan models.Point)
	for _, shard := range u.shardsColl.Shards() {
		u.points[shard.Name()] = make(chan models.Point, 1024)
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
	for _, shard := range u.shardsColl.Shards() {
		go u.batchPoints(shard)
	}

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

func (u *Collectd2HTTP) batchPoints(shard shard) {
	batch := make([]string, 0, 5000)
	batchLen := 0
	ticker := time.Tick(500 * time.Millisecond)

	submitBatch := func() {
		log.Printf("Submitting batch in shard '%s' of length: %d (%d)\n", shard.Name(), len(batch), batchLen)

		payload := []byte(strings.Join(batch, "\n") + "\n")
		wg := &sync.WaitGroup{}
		for _, b := range shard.backends {
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
			}(&b, wg)
		}
		wg.Wait()
		batch = make([]string, 0, 5000)
		batchLen = 0
	}

	var line string
	for {
		select {
		case p := <-u.points[shard.Name()]:
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
			u.sanitizePoint(point)
			shard := u.shardsColl.GetShardForPoint(point)
			if shard == nil {
				log.Print("No shard found for point: " + point.String())
				continue
			}
			u.points[shard.Name()] <- point
		}
	}

}

// Sanitize a data point. For now, if the host field contains a ':' it will
// split the host into a host and subhost field using the ':' as separator.
// TODO: Ultimately this should be done by a user-definable LUA script
func (u *Collectd2HTTP) sanitizePoint(p models.Point) {
	host, ok := p.Tags()["host"]
	if !ok {
		return
	}

	parts := strings.Split(host, ":")
	if len(parts) < 2 {
		return
	}

	p.AddTag("host", parts[0])
	p.AddTag("vhost", parts[1])
}

func (u *Collectd2HTTP) Stop() error {
	atomic.StoreInt64(&u.closing, 1)
	return u.l.Close()
}
