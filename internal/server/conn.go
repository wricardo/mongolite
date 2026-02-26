package server

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"strings"

	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type conn struct {
	nc      net.Conn
	handler *handler.Handler
}

func (c *conn) serve() {
	defer c.nc.Close()
	reader := bufio.NewReader(c.nc)

	for {
		hdr, err := proto.ReadHeader(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Printf("conn %s: read header: %v", c.nc.RemoteAddr(), err)
			}
			return
		}

		switch hdr.OpCode {
		case proto.OpMsg:
			msg, err := proto.ReadOpMsg(reader, hdr)
			if err != nil {
				log.Printf("conn %s: read OP_MSG: %v", c.nc.RemoteAddr(), err)
				return
			}
			c.handleOpMsg(msg)

		case proto.OpQuery:
			qry, err := proto.ReadOpQuery(reader, hdr)
			if err != nil {
				log.Printf("conn %s: read OP_QUERY: %v", c.nc.RemoteAddr(), err)
				return
			}
			c.handleOpQuery(qry)

		default:
			// Unsupported opcode — read and discard remaining bytes
			remaining := int(hdr.MessageLength) - 16
			if remaining > 0 {
				discard := make([]byte, remaining)
				io.ReadFull(reader, discard)
			}
			resp := bson.D{
				{Key: "ok", Value: float64(0)},
				{Key: "errmsg", Value: "unsupported opcode"},
				{Key: "code", Value: int32(2)},
			}
			proto.WriteOpMsg(c.nc, hdr.RequestID, resp)
		}
	}
}

func (c *conn) handleOpMsg(msg *proto.OpMsgRequest) {
	var body bson.Raw
	var extraSections []proto.Section
	for _, sec := range msg.Sections {
		if sec.Kind == proto.SectionBody {
			body = sec.Body
		} else {
			extraSections = append(extraSections, sec)
		}
	}

	if body == nil {
		resp := bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: "missing body section"},
		}
		proto.WriteOpMsg(c.nc, msg.Header.RequestID, resp)
		return
	}

	resp, err := c.handler.Handle(body, extraSections)
	if err != nil {
		resp = bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: err.Error()},
		}
	}

	// Check moreToCome flag — if set, client doesn't expect a response
	if msg.FlagBits&2 != 0 {
		return
	}

	if err := proto.WriteOpMsg(c.nc, msg.Header.RequestID, resp); err != nil {
		log.Printf("conn %s: write response: %v", c.nc.RemoteAddr(), err)
	}
}

func (c *conn) handleOpQuery(qry *proto.OpQueryRequest) {
	// OP_QUERY is used by drivers for the initial handshake.
	// The collection name is typically "admin.$cmd" or "<db>.$cmd".
	// The query document is the command.

	// Extract db from collection name (format: "db.$cmd")
	db := "admin"
	if idx := strings.Index(qry.FullCollectionName, "."); idx > 0 {
		db = qry.FullCollectionName[:idx]
	}

	// Add $db field to the query doc if not present
	var cmd bson.D
	if err := bson.Unmarshal(qry.Query, &cmd); err != nil {
		resp := bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: "failed to parse query: " + err.Error()},
		}
		proto.WriteOpReply(c.nc, qry.Header.RequestID, resp)
		return
	}

	// Add $db to the command so the handler can extract it
	hasDB := false
	for _, e := range cmd {
		if e.Key == "$db" {
			hasDB = true
			break
		}
	}
	if !hasDB {
		cmd = append(cmd, bson.E{Key: "$db", Value: db})
	}

	body, err := bson.Marshal(cmd)
	if err != nil {
		resp := bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: "marshal error: " + err.Error()},
		}
		proto.WriteOpReply(c.nc, qry.Header.RequestID, resp)
		return
	}

	resp, err := c.handler.Handle(bson.Raw(body), nil)
	if err != nil {
		resp = bson.D{
			{Key: "ok", Value: float64(0)},
			{Key: "errmsg", Value: err.Error()},
		}
	}

	if err := proto.WriteOpReply(c.nc, qry.Header.RequestID, resp); err != nil {
		log.Printf("conn %s: write OP_REPLY: %v", c.nc.RemoteAddr(), err)
	}
}
