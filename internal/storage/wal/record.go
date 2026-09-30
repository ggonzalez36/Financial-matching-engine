package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/fintech/matching-engine/internal/domain"
)

// WAL Binary Framing Specification:
// +-------------+-------------+---------------+---------------+--------------------+
// | Magic (2B)  | Type (1B)   | Length (2B)   | CRC32 (4B)    | Payload (NB)       |
// +-------------+-------------+---------------+---------------+--------------------+
// Total Framing Overhead: 9 bytes per record.
//
// All integers are encoded using Little-Endian format for direct hardware compatibility with x86/ARM64.

const (
	MagicBytes uint16 = 0x5741 // "WA"
	HeaderSize        = 9
)

var (
	ErrCorruptedRecord = errors.New("wal record checksum mismatch or truncated")
	ErrInvalidMagic    = errors.New("invalid wal record magic bytes")
)

type RecordType uint8

const (
	RecordOrderNew    RecordType = 0x01
	RecordOrderCancel RecordType = 0x02
	RecordTrade       RecordType = 0x03
)

// EncodeOrderNew serializes a newly placed order into compact binary form.
// Payload length: 36 bytes.
func EncodeOrderNew(o *domain.Order, buf []byte) []byte {
	if len(buf) < 36 {
		buf = make([]byte, 36)
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(o.ID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(o.Price))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(o.Quantity))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(o.Timestamp))
	buf[32] = byte(o.Side)
	buf[33] = byte(o.Type)
	buf[34] = byte(o.TIF)
	buf[35] = 0 // reserved padding
	return buf[:36]
}

// DecodeOrderNew deserializes an OrderNew payload.
func DecodeOrderNew(payload []byte, o *domain.Order) error {
	if len(payload) < 36 {
		return ErrCorruptedRecord
	}
	o.ID = domain.OrderID(binary.LittleEndian.Uint64(payload[0:8]))
	o.Price = domain.Price(binary.LittleEndian.Uint64(payload[8:16]))
	o.Quantity = domain.Quantity(binary.LittleEndian.Uint64(payload[16:24]))
	o.LeavesQty = o.Quantity
	o.Timestamp = int64(binary.LittleEndian.Uint64(payload[24:32]))
	o.Side = domain.Side(payload[32])
	o.Type = domain.OrderType(payload[33])
	o.TIF = domain.TimeInForce(payload[34])
	o.Status = domain.StatusNew
	return nil
}

// EncodeOrderCancel serializes an order cancellation event.
// Payload length: 16 bytes.
func EncodeOrderCancel(orderID domain.OrderID, timestamp int64, buf []byte) []byte {
	if len(buf) < 16 {
		buf = make([]byte, 16)
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(orderID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(timestamp))
	return buf[:16]
}

// DecodeOrderCancel deserializes an order cancellation event.
func DecodeOrderCancel(payload []byte) (domain.OrderID, int64, error) {
	if len(payload) < 16 {
		return 0, 0, ErrCorruptedRecord
	}
	orderID := domain.OrderID(binary.LittleEndian.Uint64(payload[0:8]))
	ts := int64(binary.LittleEndian.Uint64(payload[8:16]))
	return orderID, ts, nil
}

// EncodeTrade serializes an executed trade.
// Payload length: 41 bytes.
func EncodeTrade(t *domain.Trade, buf []byte) []byte {
	if len(buf) < 41 {
		buf = make([]byte, 41)
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t.MakerOrderID))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(t.TakerOrderID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(t.Price))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(t.Quantity))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(t.Timestamp))
	buf[40] = byte(t.MakerSide)
	return buf[:41]
}

// DecodeTrade deserializes an executed trade.
func DecodeTrade(payload []byte, t *domain.Trade) error {
	if len(payload) < 41 {
		return ErrCorruptedRecord
	}
	t.MakerOrderID = domain.OrderID(binary.LittleEndian.Uint64(payload[0:8]))
	t.TakerOrderID = domain.OrderID(binary.LittleEndian.Uint64(payload[8:16]))
	t.Price = domain.Price(binary.LittleEndian.Uint64(payload[16:24]))
	t.Quantity = domain.Quantity(binary.LittleEndian.Uint64(payload[24:32]))
	t.Timestamp = int64(binary.LittleEndian.Uint64(payload[32:40]))
	t.MakerSide = domain.Side(payload[40])
	return nil
}

// BuildFrame constructs the complete binary frame: Header + Checksum + Payload.
func BuildFrame(recType RecordType, payload []byte, out []byte) []byte {
	frameLen := HeaderSize + len(payload)
	if cap(out) < frameLen {
		out = make([]byte, frameLen)
	} else {
		out = out[:frameLen]
	}

	checksum := crc32.ChecksumIEEE(payload)

	binary.LittleEndian.PutUint16(out[0:2], MagicBytes)
	out[2] = byte(recType)
	binary.LittleEndian.PutUint16(out[3:5], uint16(len(payload)))
	binary.LittleEndian.PutUint32(out[5:9], checksum)
	copy(out[9:], payload)

	return out
}
