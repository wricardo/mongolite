package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var requestIDCounter atomic.Int32

func NextRequestID() int32 {
	return requestIDCounter.Add(1)
}

func WriteOpMsg(w io.Writer, responseTo int32, doc bson.D) error {
	body, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal response doc: %w", err)
	}

	// messageLength = header(16) + flagBits(4) + kind(1) + body
	msgLen := int32(16 + 4 + 1 + len(body))

	hdr := MsgHeader{
		MessageLength: msgLen,
		RequestID:     NextRequestID(),
		ResponseTo:    responseTo,
		OpCode:        OpMsg,
	}

	if err := binary.Write(w, binary.LittleEndian, &hdr); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// flagBits = 0
	if err := binary.Write(w, binary.LittleEndian, uint32(0)); err != nil {
		return fmt.Errorf("write flagBits: %w", err)
	}

	// kind 0 section
	if _, err := w.Write([]byte{SectionBody}); err != nil {
		return fmt.Errorf("write section kind: %w", err)
	}

	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("write body: %w", err)
	}

	return nil
}

// WriteOpReply writes an OP_REPLY message (used to respond to OP_QUERY).
func WriteOpReply(w io.Writer, responseTo int32, doc bson.D) error {
	body, err := bson.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal reply doc: %w", err)
	}

	// messageLength = header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + body
	msgLen := int32(16 + 4 + 8 + 4 + 4 + len(body))

	hdr := MsgHeader{
		MessageLength: msgLen,
		RequestID:     NextRequestID(),
		ResponseTo:    responseTo,
		OpCode:        OpReply,
	}

	if err := binary.Write(w, binary.LittleEndian, &hdr); err != nil {
		return fmt.Errorf("write reply header: %w", err)
	}
	// responseFlags = 0
	if err := binary.Write(w, binary.LittleEndian, int32(0)); err != nil {
		return fmt.Errorf("write reply flags: %w", err)
	}
	// cursorID = 0
	if err := binary.Write(w, binary.LittleEndian, int64(0)); err != nil {
		return fmt.Errorf("write reply cursorID: %w", err)
	}
	// startingFrom = 0
	if err := binary.Write(w, binary.LittleEndian, int32(0)); err != nil {
		return fmt.Errorf("write reply startingFrom: %w", err)
	}
	// numberReturned = 1
	if err := binary.Write(w, binary.LittleEndian, int32(1)); err != nil {
		return fmt.Errorf("write reply numberReturned: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("write reply body: %w", err)
	}
	return nil
}
