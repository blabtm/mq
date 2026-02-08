package mqtt

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/mochi-mqtt/server/v2/vcas"
	"github.com/tinylib/msgp/msgp"
)

type vClient struct {
	cid int
	cli *Client
	net *bufio.ReadWriter
	mux sync.Mutex

	oPkt vcas.Packet
	oBuf []byte
	iBuf []byte
}

func (c *vClient) vSendEmpty(t string) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.oPkt.Topic = t
	c.oPkt.Value = nil
	c.oPkt.Time = time.Now()

	out, err := vcas.Marshal(&c.oPkt, c.oBuf)

	if err != nil {
		return fmt.Errorf("vcas: marshal: %w", err)
	}

	c.net.Write(out)

	if err := c.net.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

func (c *vClient) vSendPacket(pkt *packets.Packet) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if err := vUnpack(&c.oPkt, pkt.Payload); err != nil {
		return fmt.Errorf("msgp: unpack: %w", err)
	}

	c.oPkt.Topic = pkt.TopicName
	out, err := vcas.Marshal(&c.oPkt, c.oBuf)

	if err != nil {
		return fmt.Errorf("vcas: marshal: %w", err)
	}

	c.net.Write(out)

	if err := c.net.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

func (s *Server) vAttachClient(c net.Conn) error {
	s.Listeners.ClientsWg.Add(1)
	defer s.Listeners.ClientsWg.Done()

	atomic.AddInt64(&s.Info.ClientsConnected, 1)
	defer atomic.AddInt64(&s.Info.ClientsConnected, -1)

	pkt := vcas.Packet{}
	cid := rand.Int()
	cli := &vClient{
		mux: sync.Mutex{},
		cid: cid,
		cli: s.NewClient(nil, "local", fmt.Sprintf("vcas-%d", cid), true),
		net: bufio.NewReadWriter(
			bufio.NewReader(c),
			bufio.NewWriter(c),
		),

		oBuf: make([]byte, 0, 128),
		iBuf: make([]byte, 0, 128),
	}

	s.Clients.Add(cli.cli)
	defer s.Clients.Delete(cli.cli.ID)

	callback := func(_ *Client, _ packets.Subscription, pkt packets.Packet) {
		if err := cli.vSendPacket(&pkt); err != nil {
			s.Log.Error("submit packet", "err", err)
		}
	}

	for {
		msg, err := cli.net.ReadBytes('\n')

		if err != nil {
			s.Log.Error("receive packet", "err", err)
			return err
		}

		msg = msg[:len(msg)-1]

		if err := vcas.Unmarshal(&pkt, msg); err != nil {
			s.Log.Error("receive packet", "err", fmt.Errorf("vcas: unmarshal: %w", err))
			return err
		}

		switch pkt.Method {
		case vcas.GET:
			ret := s.Topics.Messages(pkt.Topic)

			for _, p := range ret {
				if err := cli.vSendPacket(&p); err != nil {
					s.Log.Error("get packet", "err", fmt.Errorf("submit packet: %w", err))
				}
			}

			if len(ret) == 0 {
				if err := cli.vSendEmpty(pkt.Topic); err != nil {
					s.Log.Error("get packet", "err", fmt.Errorf("submit empty: %w", err))
				}
			}
		case vcas.SUB:
			if err := s.Subscribe(pkt.Topic, cid, callback); err != nil {
				s.Log.Error("subscribe", "err", err)
			}
		case vcas.USB:
			if err := s.Unsubscribe(pkt.Topic, cid); err != nil {
				s.Log.Error("unsubscribe", "err", err)
			}
		case vcas.PUB:
			out, err := vPack(&pkt, cli.iBuf)

			if err != nil {
				s.Log.Error("publish", "err", fmt.Errorf("msgp: pack: %w", err))
				return err
			}

			if err := s.InjectPacket(cli.cli, packets.Packet{
				FixedHeader: packets.FixedHeader{
					Type:   packets.Publish,
					Retain: true,
				},
				TopicName: pkt.Topic,
				Payload:   out,
			}); err != nil {
				s.Log.Error("publish", "err", fmt.Errorf("inject: %w", err))
			}
		}
	}
}

func vPack(pkt *vcas.Packet, raw []byte) ([]byte, error) {
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
		case string:
			raw = msgp.AppendString(raw, v)
		}
	}

	return raw, nil
}

func vUnpack(pkt *vcas.Packet, raw []byte) error {
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
		}

	}

	return nil
}
