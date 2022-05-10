package parser

import (
	"io"
	"strings"

	"github.com/Maki-Daisuke/cflogparser"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/mattn/go-forlines"
)

func webLogToEvent(log *cflogparser.WebLog) axiom.Event {
	return axiom.Event{
		"time":                 log.Time,
		"location":             log.Location,
		"bytes":                log.Bytes,
		"request_ip":           log.RequestIP,
		"method":               log.Method,
		"host":                 log.Host,
		"uri":                  log.URI,
		"status":               log.Status,
		"referrer":             log.Referrer,
		"user_agent":           log.UserAgent,
		"query_string":         log.QueryString,
		"cookie":               log.Cookie,
		"result_type":          log.ResultType,
		"request_id":           log.RequestID,
		"host_header":          log.HostHeader,
		"request_protocol":     log.RequestProtocol,
		"request_bytes":        log.RequestBytes,
		"time_taken":           log.TimeTaken,
		"xforwarded_for":       log.XforwardedFor,
		"ssl_protocol":         log.SslProtocol,
		"ssl_cipher":           log.SslCipher,
		"response_result_type": log.ResponseResultType,
		"http_version":         log.HTTPVersion,
		"fle_status":           log.FleStatus,
		"fle_encrypted_fields": log.FleEncryptedFields,
	}
}

func ParseCloudfrontLogs(rdr io.Reader) ([]axiom.Event, error) {
	events := make([]axiom.Event, 0, 8)

	// Read from Stdin, parse each line and count accesses to each URI
	if err := forlines.Do(rdr, func(line string) error {
		if strings.HasPrefix(line, "#") {
			// Ignore leading comment lines for meta-information
			return nil
		}
		log, err := cflogparser.ParseLineWeb(line) // ParseLineWeb returns *WebLog
		if err != nil {
			return err
		}
		ev := webLogToEvent(log)
		events = append(events, ev)
		return nil
	}); err != nil {
		return nil, err
	}

	return events, nil
}
