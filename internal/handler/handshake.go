package handler

import (
	"time"

	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("hello", cmdHello)
	Register("ismaster", cmdHello)
	Register("isMaster", cmdHello)
	Register("buildInfo", cmdBuildInfo)
	Register("buildinfo", cmdBuildInfo)
	Register("ping", cmdPing)
	Register("getParameter", cmdGetParameter)
	Register("getparameter", cmdGetParameter)
	Register("whatsmyuri", cmdWhatsMyURI)
	Register("saslStart", cmdSASLStart)
	Register("saslstart", cmdSASLStart)
	Register("saslContinue", cmdSASLContinue)
	Register("saslcontinue", cmdSASLContinue)
	Register("getLog", cmdGetLog)
	Register("getlog", cmdGetLog)
	Register("getFreeMonitoringStatus", cmdGetFreeMonitoring)
	Register("getfreemonitoringstatus", cmdGetFreeMonitoring)
	Register("endSessions", cmdEndSessions)
	Register("endsessions", cmdEndSessions)
	Register("getCmdLineOpts", cmdGetCmdLineOpts)
	Register("getcmdlineopts", cmdGetCmdLineOpts)
	Register("atlasVersion", cmdAtlasVersion)
	Register("atlasversion", cmdAtlasVersion)
	Register("serverStatus", cmdServerStatus)
	Register("serverstatus", cmdServerStatus)
	Register("connectionStatus", cmdConnectionStatus)
	Register("connectionstatus", cmdConnectionStatus)
	Register("hostInfo", cmdHostInfo)
	Register("hostinfo", cmdHostInfo)
}

func cmdHello(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "ismaster", Value: true},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "maxMessageSizeBytes", Value: int32(48000000)},
		{Key: "maxWriteBatchSize", Value: int32(100000)},
		{Key: "localTime", Value: bson.DateTime(time.Now().UnixMilli())},
		{Key: "logicalSessionTimeoutMinutes", Value: int32(30)},
		{Key: "connectionId", Value: int32(1)},
		{Key: "minWireVersion", Value: int32(0)},
		{Key: "maxWireVersion", Value: int32(21)},
		{Key: "readOnly", Value: false},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdBuildInfo(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "version", Value: "7.0.0"},
		{Key: "gitVersion", Value: "mongolite"},
		{Key: "modules", Value: bson.A{}},
		{Key: "sysInfo", Value: "mongolite"},
		{Key: "versionArray", Value: bson.A{int32(7), int32(0), int32(0), int32(0)}},
		{Key: "bits", Value: int32(64)},
		{Key: "maxBsonObjectSize", Value: int32(16777216)},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdPing(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return okResp(), nil
}

func cmdGetParameter(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdWhatsMyURI(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "you", Value: "127.0.0.1:0"},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdSASLStart(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "conversationId", Value: int32(1)},
		{Key: "done", Value: true},
		{Key: "payload", Value: bson.Binary{}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdSASLContinue(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "conversationId", Value: int32(1)},
		{Key: "done", Value: true},
		{Key: "payload", Value: bson.Binary{}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdGetLog(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "totalLinesWritten", Value: int32(0)},
		{Key: "log", Value: bson.A{}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdGetFreeMonitoring(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "state", Value: "disabled"},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdEndSessions(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return okResp(), nil
}

func cmdGetCmdLineOpts(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "argv", Value: bson.A{"mongolite"}},
		{Key: "parsed", Value: bson.D{}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdAtlasVersion(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return errorResp(59, "CommandNotFound", "no such command: 'atlasVersion'"), nil
}

func cmdServerStatus(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "host", Value: "localhost"},
		{Key: "version", Value: "7.0.0"},
		{Key: "process", Value: "mongolite"},
		{Key: "uptimeMillis", Value: int64(0)},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdConnectionStatus(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "authInfo", Value: bson.D{
			{Key: "authenticatedUsers", Value: bson.A{}},
			{Key: "authenticatedUserRoles", Value: bson.A{}},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdHostInfo(_ *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	return bson.D{
		{Key: "system", Value: bson.D{
			{Key: "hostname", Value: "localhost"},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}
