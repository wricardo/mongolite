package proto

import (
	"encoding/binary"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func ReadHeader(r io.Reader) (MsgHeader, error) {
	var h MsgHeader
	if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
		return h, fmt.Errorf("read header: %w", err)
	}
	return h, nil
}

func ReadOpMsg(r io.Reader, h MsgHeader) (*OpMsgRequest, error) {
	bodyLen := int(h.MessageLength) - 16
	if bodyLen < 5 {
		return nil, fmt.Errorf("message body too short: %d", bodyLen)
	}

	buf := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	flagBits := binary.LittleEndian.Uint32(buf[:4])
	pos := 4

	// Check if checksum is present (bit 0 of flagBits)
	checksumPresent := flagBits&1 != 0
	endPos := len(buf)
	if checksumPresent {
		endPos -= 4 // last 4 bytes are checksum, skip them
	}

	var sections []Section
	for pos < endPos {
		if pos >= len(buf) {
			break
		}
		kind := buf[pos]
		pos++

		switch kind {
		case SectionBody:
			doc, n, err := readBSONDoc(buf[pos:])
			if err != nil {
				return nil, fmt.Errorf("read body section: %w", err)
			}
			sections = append(sections, Section{Kind: SectionBody, Body: doc})
			pos += n

		case SectionDocSeq:
			if pos+4 > len(buf) {
				return nil, fmt.Errorf("truncated doc sequence size")
			}
			seqSize := int(binary.LittleEndian.Uint32(buf[pos:]))
			seqEnd := pos + seqSize // seqSize includes the 4-byte size field itself
			pos += 4

			// Read null-terminated identifier
			identEnd := pos
			for identEnd < seqEnd && buf[identEnd] != 0 {
				identEnd++
			}
			if identEnd >= seqEnd {
				return nil, fmt.Errorf("unterminated identifier in doc sequence")
			}
			identifier := string(buf[pos:identEnd])
			pos = identEnd + 1 // skip null byte

			var docs []bson.Raw
			for pos < seqEnd {
				doc, n, err := readBSONDoc(buf[pos:])
				if err != nil {
					return nil, fmt.Errorf("read doc in sequence %q: %w", identifier, err)
				}
				docs = append(docs, doc)
				pos += n
			}
			sections = append(sections, Section{
				Kind:       SectionDocSeq,
				Identifier: identifier,
				Documents:  docs,
			})

		default:
			return nil, fmt.Errorf("unknown section kind: %d", kind)
		}
	}

	return &OpMsgRequest{
		Header:   h,
		FlagBits: flagBits,
		Sections: sections,
	}, nil
}

// ReadOpQuery reads an OP_QUERY message after the header has been read.
func ReadOpQuery(r io.Reader, h MsgHeader) (*OpQueryRequest, error) {
	bodyLen := int(h.MessageLength) - 16
	if bodyLen < 13 { // flags(4) + at least 1 byte cstring + null + skip(4) + return(4)
		return nil, fmt.Errorf("OP_QUERY body too short: %d", bodyLen)
	}

	buf := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read OP_QUERY body: %w", err)
	}

	flags := int32(binary.LittleEndian.Uint32(buf[:4]))
	pos := 4

	// Read null-terminated collection name
	nameEnd := pos
	for nameEnd < len(buf) && buf[nameEnd] != 0 {
		nameEnd++
	}
	if nameEnd >= len(buf) {
		return nil, fmt.Errorf("unterminated collection name in OP_QUERY")
	}
	collName := string(buf[pos:nameEnd])
	pos = nameEnd + 1

	if pos+8 > len(buf) {
		return nil, fmt.Errorf("OP_QUERY truncated after collection name")
	}
	numberToSkip := int32(binary.LittleEndian.Uint32(buf[pos:]))
	pos += 4
	numberToReturn := int32(binary.LittleEndian.Uint32(buf[pos:]))
	pos += 4

	query, _, err := readBSONDoc(buf[pos:])
	if err != nil {
		return nil, fmt.Errorf("read OP_QUERY query doc: %w", err)
	}

	return &OpQueryRequest{
		Header:             h,
		Flags:              flags,
		FullCollectionName: collName,
		NumberToSkip:       numberToSkip,
		NumberToReturn:     numberToReturn,
		Query:              query,
	}, nil
}

// readBSONDoc reads a single BSON document from buf, returning the raw doc and bytes consumed.
func readBSONDoc(buf []byte) (bson.Raw, int, error) {
	if len(buf) < 4 {
		return nil, 0, fmt.Errorf("buffer too short for BSON doc length")
	}
	docLen := int(binary.LittleEndian.Uint32(buf[:4]))
	if docLen < 5 || docLen > len(buf) {
		return nil, 0, fmt.Errorf("invalid BSON doc length: %d (buf: %d)", docLen, len(buf))
	}
	doc := make([]byte, docLen)
	copy(doc, buf[:docLen])
	return bson.Raw(doc), docLen, nil
}
