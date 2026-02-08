package vcas

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	cases := map[string]struct {
		inp Packet
		exp struct {
			err bool
			res string
		}
	}{
		`with float`: {
			inp: Packet{
				Method: PUB,
				Topic:  "test",
				Time:   time.UnixMilli(1118505599999),
				Value:  11.06,
			},
			exp: struct {
				err bool
				res string
			}{
				err: false,
				res: "time:11.06.2005 23_59_59.999|name:test|value:11.06|descr:none|type:rw|units:none\n",
			},
		},
		`with string`: {
			inp: Packet{
				Method: PUB,
				Topic:  "test",
				Time:   time.UnixMilli(1118505599999),
				Value:  "hello",
			},
			exp: struct {
				err bool
				res string
			}{
				err: false,
				res: "time:11.06.2005 23_59_59.999|name:test|value:hello|descr:none|type:rw|units:none\n",
			},
		},
		`without value`: {
			inp: Packet{
				Method: PUB,
				Topic:  "test",
				Time:   time.UnixMilli(1118505599999),
			},
			exp: struct {
				err bool
				res string
			}{
				err: false,
				res: "time:11.06.2005 23_59_59.999|name:test|value:none|descr:none|type:rw|units:none\n",
			},
		},
		`without name`: {
			inp: Packet{
				Method: PUB,
				Time:   time.UnixMilli(1118505599999),
			},
			exp: struct {
				err bool
				res string
			}{
				err: true,
			},
		},
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			res := make([]byte, 0)
			res, err := Marshal(&data.inp, res)

			if !data.exp.err {
				assert.Nil(t, err)
				assert.Equal(t, data.exp.res, string(res))
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	for b.Loop() {
		b.StopTimer()

		pay := make([]byte, 0)
		pkt := Packet{
			Method: PUB,
			Topic:  "VEPP/CCD/1M1L/sigma_x",
			Time:   time.UnixMilli(1118505599999),
			Value:  rand.Float64(),
		}

		b.StartTimer()
		Marshal(&pkt, pay)
	}
}

func TestUnmarshal(t *testing.T) {
	cases := map[string]struct {
		inp string
		exp struct {
			err bool
			res Packet
		}
	}{
		`publish (set)`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`publish (s)`: {
			inp: "time:11.06.2005 23_59_59.999|method:s|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`subscribe`: {
			inp: "time:11.06.2005 23_59_59.999|method:subscribe|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: SUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`subscribe (subscr)`: {
			inp: "time:11.06.2005 23_59_59.999|method:subscr|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: SUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`subscribe (sb)`: {
			inp: "time:11.06.2005 23_59_59.999|method:sb|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: SUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`unsubscribe (release)`: {
			inp: "time:11.06.2005 23_59_59.999|method:release|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: USB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`unsubscribe (rel)`: {
			inp: "time:11.06.2005 23_59_59.999|method:rel|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: USB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`get (getfull)`: {
			inp: "time:11.06.2005 23_59_59.999|method:getfull|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: GET,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`get (get)`: {
			inp: "time:11.06.2005 23_59_59.999|method:get|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: GET,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`get (gf)`: {
			inp: "time:11.06.2005 23_59_59.999|method:gf|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: GET,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`get (g)`: {
			inp: "time:11.06.2005 23_59_59.999|method:g|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: GET,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`time (t)`: {
			inp: "t:11.06.2005 23_59_59.999|method:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`method (meth)`: {
			inp: "time:11.06.2005 23_59_59.999|meth:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`method (m)`: {
			inp: "time:11.06.2005 23_59_59.999|m:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`name (n)`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|n:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`value (val)`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|val:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`value (v)`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|v:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`with leading escapes`: {
			inp: "\t\t  time:11.06.2005 23_59_59.999|method:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`with trailing escapes`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|value:11.06\t\t ",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  "11.06\t\t ",
				},
			},
		},
		`with inner escapes`: {
			inp: "time:11.06.2005 23_59_59.999|\t\t method:set|name :test| value\t:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`with extra token`: {
			inp: "time:11.06.2005 23_59_59.999|extra:none|method:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  11.06,
				},
			},
		},
		`with empty value`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|value:|descr:none|type:rw|units:none",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`with none value`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|name:test|value:none",
			exp: struct {
				err bool
				res Packet
			}{
				err: false,
				res: Packet{
					Method: PUB,
					Topic:  "test",
					Time:   time.UnixMilli(1118505599999),
					Value:  nil,
				},
			},
		},
		`with malformed time`: {
			inp: "time:11.06.2005 23:59:59.999|method:set|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: true,
			},
		},
		`without name`: {
			inp: "time:11.06.2005 23_59_59.999|method:set|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: true,
			},
		},
		`without method`: {
			inp: "time:11.06.2005 23_59_59.999|name:test|value:11.06",
			exp: struct {
				err bool
				res Packet
			}{
				err: true,
			},
		},
		`with unknown method`: {
			inp: "time:11.06.2005 23_59_59.999|method:extra|name:test",
			exp: struct {
				err bool
				res Packet
			}{
				err: true,
			},
		},
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			pkt := Packet{}
			err := Unmarshal(&pkt, []byte(data.inp))

			if !data.exp.err {
				assert.Nil(t, err)
				assert.Equal(t, data.exp.res, pkt)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for b.Loop() {
		b.StopTimer()

		pkt := Packet{}
		pay := fmt.Appendf(nil,
			"time:11.06.2005 23_59_59.999|method:subscribe|name:VEPP/CCD/1M1L/sigma_x|value:%f",
			rand.Float64(),
		)

		b.StartTimer()
		Unmarshal(&pkt, pay)
	}
}
