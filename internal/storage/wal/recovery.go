package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"

	"github.com/fintech/matching-engine/internal/domain"
)

// ReplayHandler is invoked sequentially for each valid record discovered during WAL replay.
type ReplayHandler interface {
	OnOrderNew(order *domain.Order) error
	OnOrderCancel(orderID domain.OrderID, timestamp int64) error
	OnTrade(trade *domain.Trade) error
}

// Recover reads the WAL from byte 0 to EOF, verifies data integrity via CRC32,
// and deterministically replays state transitions to reconstruct the in-memory order book.
func Recover(path string, handler ReplayHandler) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // No existing log to recover
		}
		return 0, err
	}
	defer file.Close()

	headerBuf := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 512)
	recordsProcessed := 0

	for {
		// Read 9-byte header
		_, err := io.ReadFull(file, headerBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // Clean termination at log boundary
			}
			return recordsProcessed, err
		}

		// Verify Magic
		magic := binary.LittleEndian.Uint16(headerBuf[0:2])
		if magic != MagicBytes {
			return recordsProcessed, ErrInvalidMagic
		}

		recType := RecordType(headerBuf[2])
		payloadLen := int(binary.LittleEndian.Uint16(headerBuf[3:5]))
		expectedCRC := binary.LittleEndian.Uint32(headerBuf[5:9])

		// Ensure payload buffer capacity
		if len(payloadBuf) < payloadLen {
			payloadBuf = make([]byte, payloadLen)
		}
		targetPayload := payloadBuf[:payloadLen]

		// Read Payload
		if _, err := io.ReadFull(file, targetPayload); err != nil {
			return recordsProcessed, ErrCorruptedRecord
		}

		// Verify CRC32 Integrity
		actualCRC := crc32.ChecksumIEEE(targetPayload)
		if actualCRC != expectedCRC {
			return recordsProcessed, ErrCorruptedRecord
		}

		// Replay into handler
		switch recType {
		case RecordOrderNew:
			order := domain.AcquireOrder()
			if err := DecodeOrderNew(targetPayload, order); err != nil {
				return recordsProcessed, err
			}
			if err := handler.OnOrderNew(order); err != nil {
				return recordsProcessed, err
			}

		case RecordOrderCancel:
			orderID, ts, err := DecodeOrderCancel(targetPayload)
			if err != nil {
				return recordsProcessed, err
			}
			if err := handler.OnOrderCancel(orderID, ts); err != nil {
				return recordsProcessed, err
			}

		case RecordTrade:
			trade := domain.AcquireTrade()
			if err := DecodeTrade(targetPayload, trade); err != nil {
				return recordsProcessed, err
			}
			if err := handler.OnTrade(trade); err != nil {
				return recordsProcessed, err
			}
		}

		recordsProcessed++
	}

	return recordsProcessed, nil
}
