package mqtt

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/vcas"
	"github.com/tinylib/msgp/msgp"
)

type AccessMode int

const (
	RW AccessMode = iota + 1
	RO
	EX
)

var modeMap map[string]AccessMode = map[string]AccessMode{
	"rw": RW,
	"ro": RO,
	"ex": EX,
}

type VirtualChannel struct {
	Info  map[string]string
	Mode  AccessMode
	Owner atomic.Int32
}

type vServer struct {
	*Server

	vCounter      atomic.Int32
	vChannels     sync.Map
	vChannelsList strings.Builder
	vChannelsLock sync.RWMutex
}

func vNew(s *Server) *vServer {
	srv := &vServer{
		Server: s,
	}

	srv.vChannels.Store("ChannelsList", &VirtualChannel{
		Mode: RO,
		Info: map[string]string{
			"type":  "ro",
			"descr": "List of all service channels",
		},
	})

	srv.vChannels.Store("Clients", &VirtualChannel{
		Mode: RO,
		Info: map[string]string{
			"type":  "ro",
			"descr": "Number of connected clients",
		},
	})

	srv.vChannelsList.WriteString("ChannelsList,Clients")
	s.Log.Info("vcas: loaded")

	return srv
}

func (s *vServer) vFetch() error {
	s.Log.Info("vcas: initializing channels")

	ctx := context.Background()
	con, err := pgx.Connect(ctx, s.Options.DatabaseAddr)

	if err != nil {
		return fmt.Errorf("vcas: db: connect: %w", err)
	} else {
		defer con.Close(ctx)
	}

	rows, err := con.Query(ctx, "SELECT name, units, description, cas_type FROM channels")

	if err != nil {
		return fmt.Errorf("vcas: db: query: %w", err)
	} else {
		defer rows.Close()
	}

	var name, units, description, mode pgtype.Text

	_, err = pgx.ForEachRow(rows, []any{&name, &units, &description, &mode}, func() error {
		s.vChannelsList.WriteRune(',')
		s.vChannelsList.WriteString(name.String)

		if !mode.Valid {
			mode.String = "rw"
		}

		info := map[string]string{
			"type": mode.String,
		}

		if units.Valid && units.String != "" {
			info["units"] = units.String
		}

		if description.Valid && description.String != "" {
			info["descr"] = description.String
		}

		s.vChannels.Store(name.String, &VirtualChannel{
			Mode: modeMap[mode.String],
			Info: info,
		})

		return nil
	})

	if err != nil {
		return fmt.Errorf("vcas: db: scan: %w", err)
	}

	s.Log.Info("vcas: channels initialization finished")

	return nil
}

func (s *vServer) vAttach(c net.Conn) error {
	s.Listeners.ClientsWg.Add(1)
	defer s.Listeners.ClientsWg.Done()

	atomic.AddInt64(&s.Info.ClientsConnected, 1)
	defer atomic.AddInt64(&s.Info.ClientsConnected, -1)

	s.vUpdateCounter(1)
	defer s.vUpdateCounter(-1)

	cli := s.vNewClient(c)

	s.Clients.Add(cli.mqtt)
	defer s.Clients.Delete(cli.mqtt.ID)

	defer func() {
		cli.Log.Debug("unlocking")

		for _, vc := range cli.owned {
			vc.Owner.Swap(0)
			delete(vc.Info, "host")
			delete(vc.Info, "port")
		}
	}()

	defer func() {
		cli.Log.Debug("freeing", "subs", cli.subs)

		for sb := range cli.subs {
			s.Unsubscribe(sb, int(cli.cid))
		}
	}()

	extra := make(map[string]string)

	for {
		msg, err := cli.buf.ReadBytes('\n')

		if err != nil {
			cli.Log.Error("read", "err", err)
			return fmt.Errorf("read: %w", err)
		}

		msg = msg[:len(msg)-1]
		cli.Log.Debug("income", "msg", msg)

		if err := vcas.Unmarshal(&cli.iPkt, msg, extra); err != nil {
			cli.Log.Error("receive packet", "err", fmt.Errorf("vcas: unmarshal: %w", err))
			continue
		}

		switch cli.iPkt.Method {
		case vcas.SET:
			if err := cli.set(); err != nil {
				cli.Log.Error("set", "err", err)
			}
		case vcas.GET:
			if err := cli.get(); err != nil {
				cli.Log.Error("get", "err", err)
			}
		case vcas.GETFULL:
			if err := cli.getFull(); err != nil {
				cli.Log.Error("get full", "err", err)
			}
		case vcas.SUB:
			if err := cli.subscribe(); err != nil {
				cli.Log.Error("subscribe", "err", err)
			}
		case vcas.UNSUB:
			if err := cli.unsubscribe(); err != nil {
				cli.Log.Error("unsubscribe", "err", err)
			}
		case vcas.CREATE:
			if err := cli.create(extra); err != nil {
				cli.Log.Error("create", "err", err)
			}

			extra = make(map[string]string)
		case vcas.FREE:
			if err := cli.free(); err != nil {
				cli.Log.Error("free", "err", err)
			}
		}
	}
}

func (s *vServer) vNewClient(c net.Conn) *vClient {
	addr := strings.Split(c.RemoteAddr().String(), ":")
	cid := rand.Int31()

	cli := &vClient{
		Log:  s.Log.With("addr", c.RemoteAddr().String()),
		srv:  s,
		cid:  cid,
		host: addr[0],
		port: addr[1],
		mqtt: s.NewClient(nil, "local", fmt.Sprintf("vcas-%d", cid), true),
		buf: bufio.NewReadWriter(
			bufio.NewReader(c),
			bufio.NewWriter(c),
		),
		owned: make(map[string]*VirtualChannel),
		subs:  make(map[string]struct{}),
		oBuf:  make([]byte, 0, 128),
		iBuf:  make([]byte, 0, 128),
	}

	cli.subFn = func(_ *Client, _ packets.Subscription, pkt packets.Packet) {
		cli.Log.Debug("callback triggered", "topic", pkt.TopicName)

		if err := cli.sendPacket(&pkt, nil); err != nil {
			cli.Log.Error("submit packet", "err", err)
		}
	}

	return cli
}

func (s *vServer) vUpdateCounter(d int32) error {
	new := s.vCounter.Add(d)
	raw, _ := Pack(&vcas.Packet{
		Time:  time.Now(),
		Value: int64(new),
	}, nil)

	if err := s.Publish("Clients", raw, true, 0); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

type vClient struct {
	Log *slog.Logger

	srv   *vServer
	cid   int32
	host  string
	port  string
	mqtt  *Client
	buf   *bufio.ReadWriter
	owned map[string]*VirtualChannel
	subs  map[string]struct{}
	mux   sync.Mutex
	subFn InlineSubFn
	oPkt  vcas.Packet
	iPkt  vcas.Packet
	oBuf  []byte
	iBuf  []byte
}

func (c *vClient) set() error {
	avc, ok := c.srv.vChannels.Load(c.iPkt.Topic)
	pass := true

	if ok {
		vc := avc.(*VirtualChannel)

		switch vc.Mode {
		case RO:
			pass = false
		case EX:
			upd := vc.Owner.CompareAndSwap(0, c.cid)

			if vc.Owner.Load() == c.cid {
				if upd {
					c.owned[c.iPkt.Topic] = vc
					vc.Info["host"] = c.host
					vc.Info["port"] = c.port

					c.Log.Debug("locked", "topic", c.iPkt.Topic)
				}
			} else {
				pass = false
			}
		}
	}

	if !pass {
		return nil
	}

	out, err := Pack(&c.iPkt, c.iBuf)

	if err != nil {
		return fmt.Errorf("msgp: pack: %w", err)
	}

	if err := c.srv.InjectPacket(c.mqtt, packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type:   packets.Publish,
			Retain: true,
		},
		TopicName: c.iPkt.Topic,
		Payload:   out,
	}); err != nil {
		return fmt.Errorf("inject: %w", err)
	}

	return nil
}

func (c *vClient) get() error {
	if c.iPkt.Topic == "ChannelsList" {
		if err := c.sendChannels(nil); err != nil {
			c.Log.Error("get", "err", fmt.Errorf("submit: %w", err))
		}

		return nil
	}

	ret := c.srv.Topics.Messages(c.iPkt.Topic)

	for _, p := range ret {
		if err := c.sendPacket(&p, nil); err != nil {
			c.Log.Error("get", "err", fmt.Errorf("submit: %w", err))
		}
	}

	if len(ret) == 0 {
		if err := c.sendEmpty(c.iPkt.Topic, nil); err != nil {
			c.Log.Error("get", "err", fmt.Errorf("submit: %w", err))
		}
	}

	return nil
}

func (c *vClient) getFull() error {
	if c.iPkt.Topic == "ChannelsList" {
		vc, _ := c.srv.vChannels.Load("ChannelsList")

		if err := c.sendChannels(vc.(*VirtualChannel).Info); err != nil {
			c.Log.Error("get full", "err", fmt.Errorf("submit: %w", err))
		}

		return nil
	}

	var ext map[string]string = nil
	var vc, ok = c.srv.vChannels.Load(c.iPkt.Topic)

	if ok {
		ext = vc.(*VirtualChannel).Info
	}

	ret := c.srv.Topics.Messages(c.iPkt.Topic)

	for _, p := range ret {
		if err := c.sendPacket(&p, ext); err != nil {
			c.Log.Error("get full", "err", fmt.Errorf("submit: %w", err))
		}
	}

	if len(ret) == 0 {
		if err := c.sendEmpty(c.iPkt.Topic, ext); err != nil {
			c.Log.Error("get full", "err", fmt.Errorf("submit: %w", err))
		}
	}

	return nil
}

func (c *vClient) subscribe() error {
	vc, ok := c.srv.vChannels.Load(c.iPkt.Topic)

	if ok {
		ext := vc.(*VirtualChannel).Info
		ret := c.srv.Topics.Messages(c.iPkt.Topic)

		for _, p := range ret {
			if err := c.sendPacket(&p, ext); err != nil {
				c.Log.Error("subscribe", "err", fmt.Errorf("submit: %w", err))
			}
		}

		if len(ret) == 0 {
			if err := c.sendEmpty(c.iPkt.Topic, ext); err != nil {
				c.Log.Error("subscribe", "err", fmt.Errorf("submit: %w", err))
			}
		}
	}

	err := c.srv.Subscribe(c.iPkt.Topic, int(c.cid), c.subFn)

	if err == nil {
		c.subs[c.iPkt.Topic] = struct{}{}
	}

	return err
}

func (c *vClient) unsubscribe() error {
	err := c.srv.Unsubscribe(c.iPkt.Topic, int(c.cid))

	if err == nil {
		delete(c.subs, c.iPkt.Topic)
	}

	return err
}

func (c *vClient) create(extra map[string]string) error {
	_, ok := extra["type"]

	if !ok {
		extra["type"] = "rw"
	}

	_, loaded := c.srv.vChannels.LoadOrStore(c.iPkt.Topic, &VirtualChannel{
		Info: extra,
		Mode: modeMap[extra["type"]],
	})

	if !loaded {
		c.srv.vChannelsLock.Lock()
		c.srv.vChannelsList.WriteRune(',')
		c.srv.vChannelsList.WriteString(c.iPkt.Topic)
		c.srv.vChannelsLock.Unlock()
	}

	return nil
}

func (c *vClient) free() error {
	avc, ok := c.srv.vChannels.Load(c.iPkt.Topic)

	if !ok {
		return nil
	}

	vc := avc.(*VirtualChannel)

	if vc.Mode == EX && vc.Owner.CompareAndSwap(c.cid, 0) {
		delete(c.owned, c.iPkt.Topic)
		delete(vc.Info, "host")
		delete(vc.Info, "port")
	}

	return nil
}

func (c *vClient) send(extra map[string]string) error {
	out, err := vcas.Marshal(&c.oPkt, c.oBuf, extra)

	if err != nil {
		return fmt.Errorf("vcas: marshal: %w", err)
	}

	c.buf.Write(out)

	if err := c.buf.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

func (c *vClient) sendChannels(extra map[string]string) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.oPkt.Time = time.Now()
	c.oPkt.Topic = "ChannelsList"

	c.srv.vChannelsLock.RLock()
	c.oPkt.Value = c.srv.vChannelsList.String()
	c.srv.vChannelsLock.RUnlock()

	return c.send(extra)
}

func (c *vClient) sendEmpty(t string, extra map[string]string) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.oPkt.Time = time.Now()
	c.oPkt.Topic = t
	c.oPkt.Value = nil

	return c.send(extra)
}

func (c *vClient) sendPacket(pkt *packets.Packet, extra map[string]string) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := Unpack(&c.oPkt, pkt.Payload); err != nil {
		return fmt.Errorf("msgp: unpack: %w", err)
	}

	c.oPkt.Topic = pkt.TopicName

	return c.send(extra)
}

func Pack(pkt *vcas.Packet, raw []byte) ([]byte, error) {
	var num uint32 = 1

	if pkt.Value != nil {
		num += 1
	}

	raw = msgp.AppendMapHeader(raw, num)
	raw = msgp.AppendString(raw, "time")
	raw = msgp.AppendInt64(raw, pkt.Time.UnixMilli())

	if pkt.Value != nil {
		raw = msgp.AppendString(raw, "value")

		switch v := pkt.Value.(type) {
		case float64:
			raw = msgp.AppendFloat64(raw, v)
		case int64:
			raw = msgp.AppendInt64(raw, v)
		case string:
			raw = msgp.AppendString(raw, v)
		}
	}

	return raw, nil
}

func Unpack(pkt *vcas.Packet, raw []byte) error {
	pkt.Time = time.Now()
	pkt.Value = nil

	bt := msgp.Locate("time", raw)

	if len(bt) != 0 {
		t, _, err := msgp.ReadInt64Bytes(bt)

		if err != nil {
			return err
		}

		pkt.Time = time.UnixMilli(t)
	}

	bv := msgp.Locate("value", raw)

	if len(bv) != 0 {
		switch msgp.NextType(bv) {
		case msgp.StrType:
			v, _, err := msgp.ReadStringBytes(bv)

			if err != nil {
				return err
			}

			pkt.Value = v
		case msgp.Float64Type:
			v, _, err := msgp.ReadFloat64Bytes(bv)

			if err != nil {
				return err
			}

			pkt.Value = v
		case msgp.IntType:
			v, _, err := msgp.ReadInt64Bytes(bv)

			if err != nil {
				return err
			}

			pkt.Value = v
		}

	}

	return nil
}
