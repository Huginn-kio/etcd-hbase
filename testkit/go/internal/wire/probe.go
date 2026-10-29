package wire

import (
	"context"
	"errors"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"io"
	"time"
)

const protocolRevision int64 = 1 << 54

var (
	binaryKey = []byte{0x00, 0x00, 0xff}
	probeKeys = map[byte][]byte{
		'a': {0x61, 0x00, 0xff},
		'p': {0x70, 0x00, 0xff},
		'u': {0x75, 0x00, 0xff},
		'r': {0x72, 0x00, 0xff},
		'd': {0x64, 0x00, 0xff},
	}
)

type Options struct {
	SkeletonEndpoint string
	FixtureEndpoint  string
	Timeout          time.Duration
}

func (options Options) Validate() error {
	if options.SkeletonEndpoint == "" {
		return errors.New("skeleton endpoint is required")
	}
	if options.FixtureEndpoint == "" {
		return errors.New("fixture endpoint is required")
	}
	if options.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return nil
}

// Run exercises the real Java skeleton and the separate test-only failure fixture.
// Every expected failure is recorded before its status and sentinel are checked.
func Run(ctx context.Context, options Options, output io.Writer) error {
	if ctx == nil {
		return errors.New("probe context is required")
	}
	if output == nil {
		return errors.New("trace output is required")
	}
	if err := options.Validate(); err != nil {
		return err
	}

	skeleton, err := client.New(client.Options{Endpoint: options.SkeletonEndpoint, Timeout: options.Timeout})
	if err != nil {
		return errors.New("skeleton client initialization failed")
	}
	defer func() { _ = skeleton.Close() }()
	fixture, err := client.New(client.Options{Endpoint: options.FixtureEndpoint, Timeout: options.Timeout})
	if err != nil {
		return errors.New("failure fixture client initialization failed")
	}
	defer func() { _ = fixture.Close() }()

	if err := probeSkeletonUnary(ctx, options, skeleton, output); err != nil {
		return err
	}
	if err := probeSkeletonWatch(ctx, options, skeleton, output); err != nil {
		return err
	}
	if err := probeSkeletonLease(ctx, options, skeleton, output); err != nil {
		return err
	}
	if err := probeWatchDeadline(ctx, options, fixture, output); err != nil {
		return err
	}
	if err := probeFixtureErrors(ctx, options, fixture, output); err != nil {
		return err
	}
	return nil
}
