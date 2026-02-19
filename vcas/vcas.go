package vcas

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

const (
	Stamp = "02.01.2006 15_04_05.000"

	SET Method = iota + 1
	GET
	GETFULL
	SUB
	UNSUB
	CREATE
	FREE

	OuterSep = '|'
	InnerSep = ':'
)

type Method int

func (m *Method) unmarshal(s string) error {
	switch s {
	case "s", "set":
		*m = SET
	case "sb", "subscr", "subscribe":
		*m = SUB
	case "rel", "release":
		*m = UNSUB
	case "g", "get":
		*m = GET
	case "gf", "getfull":
		*m = GETFULL
	case "cr", "create":
		*m = CREATE
	case "f", "free":
		*m = FREE
	default:
		return fmt.Errorf("unknown: %v", s)
	}

	return nil
}

type Packet struct {
	Time   time.Time
	Topic  string
	Method Method
	Value  any
}

func Marshal(pkt *Packet, pay []byte, extra map[string]string) ([]byte, error) {
	if pkt.Topic == "" {
		return nil, fmt.Errorf("name is missing, but required")
	}

	val := "none"

	if pkt.Value != nil {
		switch v := pkt.Value.(type) {
		case float64:
			val = strconv.FormatFloat(v, 'f', -1, 64)
		case int64:
			val = strconv.FormatInt(v, 10)
		case string:
			val = v
		default:
			return nil, fmt.Errorf("unsupported value type")
		}
	}

	buf := bytes.NewBuffer(pay)

	buf.Grow(40 + len(pkt.Topic) + len(val))
	buf.WriteString("time:")
	buf.WriteString(pkt.Time.Format(Stamp))
	buf.WriteString("|name:")
	buf.WriteString(pkt.Topic)
	buf.WriteString("|val:")
	buf.WriteString(val)

	for k, v := range extra {
		buf.WriteRune('|')
		buf.WriteString(k)
		buf.WriteRune(':')
		buf.WriteString(v)
	}

	buf.WriteRune('\n')

	return buf.Bytes(), nil
}

func Unmarshal(pkt *Packet, pay []byte, extra map[string]string) error {
	pkt.Topic = ""
	pkt.Method = 0
	pkt.Time = time.Now()
	pkt.Value = nil

	for tok := range bytes.SplitSeq(pay, []byte{OuterSep}) {
		tok := bytes.SplitN(tok, []byte{InnerSep}, 2)

		if len(tok) != 2 {
			continue
		}

		k := string(bytes.Trim(tok[0], "\n\t\r "))
		v := string(tok[1])

		switch k {
		case "method", "meth", "m":
			if err := pkt.Method.unmarshal(v); err != nil {
				return fmt.Errorf("method: %w", err)
			}
		case "time", "t":
			time, err := time.ParseInLocation(Stamp, v, time.Local)

			if err != nil {
				return fmt.Errorf("time: %v", err)
			}

			pkt.Time = time
		case "name", "n":
			pkt.Topic = v
		case "value", "val", "v":
			if v == "none" || v == "" {
				break
			}

			i64, err := strconv.ParseInt(v, 10, 64)

			if err == nil {
				pkt.Value = i64
				break
			}

			f64, err := strconv.ParseFloat(v, 64)

			if err == nil {
				pkt.Value = f64
				break
			}

			pkt.Value = v
		default:
			if extra != nil {
				extra[k] = v
			}
		}
	}

	if pkt.Topic == "" {
		return fmt.Errorf("name is missing, but required")
	}

	if pkt.Method == 0 {
		return fmt.Errorf("method is missing, but required")
	}

	return nil
}
