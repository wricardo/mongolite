package proto

import "go.mongodb.org/mongo-driver/v2/bson"

const (
	OpReply       int32 = 1
	OpQuery       int32 = 2004
	OpMsg         int32 = 2013
	SectionBody   byte  = 0
	SectionDocSeq byte  = 1
)

type MsgHeader struct {
	MessageLength int32
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

type Section struct {
	Kind       byte
	Body       bson.Raw   // Kind 0
	Identifier string     // Kind 1
	Documents  []bson.Raw // Kind 1
}

type OpMsgRequest struct {
	Header   MsgHeader
	FlagBits uint32
	Sections []Section
}

type OpQueryRequest struct {
	Header               MsgHeader
	Flags                int32
	FullCollectionName   string
	NumberToSkip         int32
	NumberToReturn       int32
	Query                bson.Raw
}
