package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"io"
	"strconv"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type rangeCommand struct {
	endpoint, caseID, tracePath, mode, scope string
	timeout                                  time.Duration
	request                                  *etcdserverpb.RangeRequest
}

func parseRange(args []string, stderr io.Writer) (rangeCommand, error) {
	var command rangeCommand
	flags := flag.NewFlagSet("m0driver", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&command.endpoint, "endpoint", "127.0.0.1:2379", "etcd-compatible gRPC endpoint")
	flags.StringVar(&command.caseID, "case", "range-bytes", "trace case identifier")
	flags.StringVar(&command.tracePath, "trace", "-", "JSONL output path, or - for stdout")
	flags.StringVar(&command.mode, "mode", "clientv3", "clientv3 or raw apiv3 stub")
	flags.StringVar(&command.scope, "scope", "transport", "transport or transport-fixture")
	flags.DurationVar(&command.timeout, "timeout", 5*time.Second, "request deadline")
	key := flags.String("key-b64", "YQD/", "request key as base64")
	end := flags.String("range-end-b64", "", "optional range end as base64")
	revision := flags.String("revision", "0", "canonical non-negative revision as decimal int64")
	request := &etcdserverpb.RangeRequest{}
	flags.BoolVar(&request.CountOnly, "count-only", false, "send a CountOnly Range")
	flags.Int64Var(&request.Limit, "limit", 0, "optional non-negative Range limit")
	if err := flags.Parse(args); err != nil {
		return command, err
	}
	if flags.NArg() != 0 {
		return command, errors.New("unexpected positional arguments")
	}
	if command.mode != "clientv3" && command.mode != "raw" {
		return command, errors.New("mode must be clientv3 or raw")
	}
	if command.scope != "transport" && command.scope != "transport-fixture" {
		return command, errors.New("scope must be transport or transport-fixture for the Go driver")
	}
	if command.timeout <= 0 || command.timeout > 30*time.Second {
		return command, errors.New("timeout must be positive and at most 30s")
	}
	if request.Limit < 0 {
		return command, errors.New("limit cannot be negative")
	}
	if len(*key) > 1<<20 || len(*end) > 1<<20 {
		return command, errors.New("encoded key exceeds driver budget")
	}
	var err error
	request.Key, err = base64.StdEncoding.Strict().DecodeString(*key)
	if err != nil {
		return command, errors.New("key-b64 is invalid base64")
	}
	request.RangeEnd, err = base64.StdEncoding.Strict().DecodeString(*end)
	if err != nil {
		return command, errors.New("range-end-b64 is invalid base64")
	}
	request.Revision, err = strconv.ParseInt(*revision, 10, 64)
	if err != nil || request.Revision < 0 || strconv.FormatInt(request.Revision, 10) != *revision {
		return command, errors.New("revision must be a canonical non-negative int64")
	}
	command.request = request
	return command, nil
}

func (command rangeCommand) call(ctx context.Context, connection *clientv3.Client) (*etcdserverpb.RangeResponse, error) {
	request := command.request
	if command.mode == "raw" {
		return client.RawKV(connection).Range(ctx, request)
	}
	options := make([]clientv3.OpOption, 0, 4)
	if len(request.RangeEnd) == 1 && request.RangeEnd[0] == 0 {
		options = append(options, clientv3.WithFromKey())
	} else if len(request.RangeEnd) != 0 {
		options = append(options, clientv3.WithRange(string(request.RangeEnd)))
	}
	if request.Revision != 0 {
		options = append(options, clientv3.WithRev(request.Revision))
	}
	if request.CountOnly {
		options = append(options, clientv3.WithCountOnly())
	}
	if request.Limit != 0 {
		options = append(options, clientv3.WithLimit(request.Limit))
	}
	response, err := connection.KV.Get(ctx, string(request.Key), options...)
	return (*etcdserverpb.RangeResponse)(response), err
}
