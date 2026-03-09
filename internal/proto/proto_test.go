package proto

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---- helpers ----------------------------------------------------------------

// buildOpMsgBytes constructs a raw OP_MSG wire message.
// flagBits is written as-is (caller controls checksum bit).
// sections is the raw section bytes that follow flagBits.
// If appendChecksum is true, 4 zero-bytes are appended as a fake checksum.
func buildOpMsgBytes(flagBits uint32, sectionBytes []byte, appendChecksum bool) []byte {
	var buf bytes.Buffer

	body := make([]byte, 4)
	binary.LittleEndian.PutUint32(body, flagBits)
	body = append(body, sectionBytes...)
	if appendChecksum {
		body = append(body, 0, 0, 0, 0)
	}

	msgLen := int32(16 + len(body))
	hdr := MsgHeader{
		MessageLength: msgLen,
		RequestID:     1,
		ResponseTo:    0,
		OpCode:        OpMsg,
	}
	_ = binary.Write(&buf, binary.LittleEndian, &hdr)
	buf.Write(body)
	return buf.Bytes()
}

// sectionBodyBytes returns the raw bytes for a Kind-0 (body) section.
func sectionBodyBytes(doc bson.D) []byte {
	raw, err := bson.Marshal(doc)
	if err != nil {
		panic(err)
	}
	var b bytes.Buffer
	b.WriteByte(SectionBody)
	b.Write(raw)
	return b.Bytes()
}

// sectionDocSeqBytes builds a Kind-1 (document-sequence) section.
func sectionDocSeqBytes(identifier string, docs []bson.D) []byte {
	var inner bytes.Buffer
	inner.WriteString(identifier)
	inner.WriteByte(0) // null terminator
	for _, d := range docs {
		raw, err := bson.Marshal(d)
		if err != nil {
			panic(err)
		}
		inner.Write(raw)
	}
	// seqSize includes the 4-byte size field itself
	seqSize := uint32(4 + inner.Len())

	var b bytes.Buffer
	b.WriteByte(SectionDocSeq)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, seqSize)
	b.Write(sizeBuf)
	b.Write(inner.Bytes())
	return b.Bytes()
}

// buildOpQueryBytes constructs a raw OP_QUERY wire message.
func buildOpQueryBytes(flags int32, collName string, skip, ret int32, query bson.D) []byte {
	var body bytes.Buffer

	flagBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(flagBuf, uint32(flags))
	body.Write(flagBuf)

	body.WriteString(collName)
	body.WriteByte(0) // null terminator

	skipBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(skipBuf, uint32(skip))
	body.Write(skipBuf)

	retBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(retBuf, uint32(ret))
	body.Write(retBuf)

	raw, err := bson.Marshal(query)
	if err != nil {
		panic(err)
	}
	body.Write(raw)

	msgLen := int32(16 + body.Len())
	var buf bytes.Buffer
	hdr := MsgHeader{
		MessageLength: msgLen,
		RequestID:     42,
		ResponseTo:    0,
		OpCode:        OpQuery,
	}
	_ = binary.Write(&buf, binary.LittleEndian, &hdr)
	buf.Write(body.Bytes())
	return buf.Bytes()
}

// ---- Test 1: ReadOpMsg round-trip via WriteOpMsg ----------------------------

func TestReadOpMsg_RoundTrip(t *testing.T) {
	doc := bson.D{{Key: "find", Value: "testcol"}, {Key: "filter", Value: bson.D{}}}

	var buf bytes.Buffer
	if err := WriteOpMsg(&buf, 0, doc); err != nil {
		t.Fatalf("WriteOpMsg: %v", err)
	}

	r := bytes.NewReader(buf.Bytes())
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	if hdr.OpCode != OpMsg {
		t.Errorf("OpCode: got %d want %d", hdr.OpCode, OpMsg)
	}

	msg, err := ReadOpMsg(r, hdr)
	if err != nil {
		t.Fatalf("ReadOpMsg: %v", err)
	}

	if msg.FlagBits != 0 {
		t.Errorf("FlagBits: got %d want 0", msg.FlagBits)
	}
	if len(msg.Sections) != 1 {
		t.Fatalf("len(Sections): got %d want 1", len(msg.Sections))
	}
	sec := msg.Sections[0]
	if sec.Kind != SectionBody {
		t.Errorf("section Kind: got %d want %d", sec.Kind, SectionBody)
	}
	if sec.Body == nil {
		t.Fatal("section Body is nil")
	}

	// Decode and verify field
	var got bson.D
	if err := bson.Unmarshal(sec.Body, &got); err != nil {
		t.Fatalf("Unmarshal body: %v", err)
	}
	if len(got) < 1 || got[0].Key != "find" {
		t.Errorf("decoded doc first key: got %v want 'find'", got)
	}

	// header fields are propagated
	if msg.Header.MessageLength != hdr.MessageLength {
		t.Errorf("Header.MessageLength mismatch")
	}
}

// ---- Test 2: ReadOpMsg with checksum bit set --------------------------------

func TestReadOpMsg_ChecksumBit(t *testing.T) {
	doc := bson.D{{Key: "ping", Value: int32(1)}}
	secBytes := sectionBodyBytes(doc)

	// flagBits bit-0 = 1  → checksum present; appendChecksum = true
	raw := buildOpMsgBytes(0x00000001, secBytes, true)

	r := bytes.NewReader(raw)
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	msg, err := ReadOpMsg(r, hdr)
	if err != nil {
		t.Fatalf("ReadOpMsg with checksum: %v", err)
	}

	if msg.FlagBits&1 == 0 {
		t.Errorf("FlagBits checksum bit not set: got %d", msg.FlagBits)
	}
	if len(msg.Sections) != 1 {
		t.Fatalf("sections: got %d want 1", len(msg.Sections))
	}
	if msg.Sections[0].Kind != SectionBody {
		t.Errorf("Kind: got %d want %d", msg.Sections[0].Kind, SectionBody)
	}
}

// ---- Test 3: ReadOpMsg Kind-1 (SectionDocSeq) with multiple docs ------------

func TestReadOpMsg_DocSeq(t *testing.T) {
	docs := []bson.D{
		{{Key: "_id", Value: int32(1)}, {Key: "x", Value: "alpha"}},
		{{Key: "_id", Value: int32(2)}, {Key: "x", Value: "beta"}},
		{{Key: "_id", Value: int32(3)}, {Key: "x", Value: "gamma"}},
	}
	identifier := "documents"

	secBytes := sectionDocSeqBytes(identifier, docs)
	raw := buildOpMsgBytes(0, secBytes, false)

	r := bytes.NewReader(raw)
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	msg, err := ReadOpMsg(r, hdr)
	if err != nil {
		t.Fatalf("ReadOpMsg DocSeq: %v", err)
	}

	if len(msg.Sections) != 1 {
		t.Fatalf("sections: got %d want 1", len(msg.Sections))
	}
	sec := msg.Sections[0]
	if sec.Kind != SectionDocSeq {
		t.Errorf("Kind: got %d want %d (SectionDocSeq)", sec.Kind, SectionDocSeq)
	}
	if sec.Identifier != identifier {
		t.Errorf("Identifier: got %q want %q", sec.Identifier, identifier)
	}
	if len(sec.Documents) != len(docs) {
		t.Fatalf("Documents count: got %d want %d", len(sec.Documents), len(docs))
	}

	// Verify each doc round-trips
	for i, wantDoc := range docs {
		var got bson.D
		if err := bson.Unmarshal(sec.Documents[i], &got); err != nil {
			t.Fatalf("Unmarshal doc[%d]: %v", i, err)
		}
		if len(got) != len(wantDoc) {
			t.Errorf("doc[%d] field count: got %d want %d", i, len(got), len(wantDoc))
		}
	}
}

// ---- Test 4: ReadOpMsg truncated body returns error -------------------------

func TestReadOpMsg_TruncatedBody(t *testing.T) {
	// Build a valid message first, then truncate it.
	doc := bson.D{{Key: "insert", Value: "col"}}
	var buf bytes.Buffer
	if err := WriteOpMsg(&buf, 0, doc); err != nil {
		t.Fatalf("WriteOpMsg: %v", err)
	}

	full := buf.Bytes()
	// Keep the full 16-byte header but provide only half the body.
	truncated := full[:len(full)/2]
	if len(truncated) <= 16 {
		// Make sure we at least have the header but a short body
		truncated = full[:17]
	}

	r := bytes.NewReader(truncated)
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	// The header still claims the full message length, but the reader is short.
	_, err = ReadOpMsg(r, hdr)
	if err == nil {
		t.Fatal("expected error for truncated body, got nil")
	}
}

// ---- Test 5: ReadOpQuery round-trip ----------------------------------------

func TestReadOpQuery_RoundTrip(t *testing.T) {
	collName := "testdb.mycollection"
	flags := int32(4) // tailable cursor flag
	skip := int32(10)
	ret := int32(20)
	query := bson.D{{Key: "status", Value: "active"}, {Key: "count", Value: int32(5)}}

	raw := buildOpQueryBytes(flags, collName, skip, ret, query)

	r := bytes.NewReader(raw)
	hdr, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}

	if hdr.OpCode != OpQuery {
		t.Errorf("OpCode: got %d want %d", hdr.OpCode, OpQuery)
	}

	req, err := ReadOpQuery(r, hdr)
	if err != nil {
		t.Fatalf("ReadOpQuery: %v", err)
	}

	if req.Flags != flags {
		t.Errorf("Flags: got %d want %d", req.Flags, flags)
	}
	if req.FullCollectionName != collName {
		t.Errorf("FullCollectionName: got %q want %q", req.FullCollectionName, collName)
	}
	if req.NumberToSkip != skip {
		t.Errorf("NumberToSkip: got %d want %d", req.NumberToSkip, skip)
	}
	if req.NumberToReturn != ret {
		t.Errorf("NumberToReturn: got %d want %d", req.NumberToReturn, ret)
	}

	var gotQuery bson.D
	if err := bson.Unmarshal(req.Query, &gotQuery); err != nil {
		t.Fatalf("Unmarshal query: %v", err)
	}
	if len(gotQuery) != len(query) {
		t.Errorf("query fields: got %d want %d", len(gotQuery), len(query))
	}
}

// ---- Test 6: ReadOpQuery truncated input returns error ----------------------

func TestReadOpQuery_TruncatedInput(t *testing.T) {
	tests := []struct {
		name  string
		bytes []byte
	}{
		{
			// Header only — body length is > actual remaining bytes
			name: "header_only",
			bytes: func() []byte {
				hdr := MsgHeader{
					MessageLength: int32(16 + 30), // claims 30 body bytes
					RequestID:     1,
					ResponseTo:    0,
					OpCode:        OpQuery,
				}
				var buf bytes.Buffer
				_ = binary.Write(&buf, binary.LittleEndian, &hdr)
				// write only 5 body bytes (less than claimed 30)
				buf.Write([]byte{0, 0, 0, 0, 'a'})
				return buf.Bytes()
			}(),
		},
		{
			// Body length field in header is below minimum (< 13)
			name: "body_too_short_header",
			bytes: func() []byte {
				hdr := MsgHeader{
					MessageLength: int32(16 + 5), // claims only 5-byte body
					RequestID:     1,
					ResponseTo:    0,
					OpCode:        OpQuery,
				}
				var buf bytes.Buffer
				_ = binary.Write(&buf, binary.LittleEndian, &hdr)
				buf.Write([]byte{0, 0, 0, 0, 0}) // 5 body bytes
				return buf.Bytes()
			}(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := bytes.NewReader(tc.bytes)
			hdr, err := ReadHeader(r)
			if err != nil {
				// If even reading the header fails (too short), that's also an error path.
				return
			}
			_, err = ReadOpQuery(r, hdr)
			if err == nil {
				t.Fatalf("expected error for truncated input, got nil")
			}
		})
	}
}

// ---- Bonus: ReadHeader itself on truncated input ----------------------------

func TestReadHeader_Truncated(t *testing.T) {
	// Only 8 bytes — not enough for a 16-byte header
	r := bytes.NewReader([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00})
	_, err := ReadHeader(r)
	if err == nil {
		t.Fatal("expected error for short header, got nil")
	}
}
