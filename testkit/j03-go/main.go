// J03 checks transport and error contracts only; it does not implement a replay driver.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var binaryKey = []byte{0x00, 0xff, 0x61}
var binaryEnd = []byte{0x00, 0xff, 0x62}

const largeRevision int64 = 1 << 54

func main() {
	endpoint := flag.String("endpoint", "", "Java skeleton loopback endpoint")
	fixture := flag.String("fixture-endpoint", "", "test-only Java error fixture endpoint")
	timeout := flag.Duration("timeout", 3*time.Second, "per-call deadline")
	flag.Parse()
	if *endpoint == "" || *fixture == "" || *timeout <= 0 || *timeout > 10*time.Second || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "endpoint, fixture-endpoint and a timeout in (0, 10s] are required")
		os.Exit(2)
	}
	if err := run(*endpoint, *fixture, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("J03 Go/Java smoke passed: skeleton unary/watch/keepalive/auth, bytes/int64, error sentinels and deadlines")
}

func run(endpoint, fixtureEndpoint string, timeout time.Duration) error {
	skeleton, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: timeout})
	if err != nil {
		return errors.New("skeleton client initialization failed")
	}
	defer skeleton.Close()
	fixture, err := clientv3.New(clientv3.Config{Endpoints: []string{fixtureEndpoint}, DialTimeout: timeout})
	if err != nil {
		return errors.New("test fixture client initialization failed")
	}
	defer fixture.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	response, callErr := pb.NewKVClient(skeleton.ActiveConnection()).Range(ctx, rangeRequest())
	cancel()
	if err := requireFailure("skeleton raw Range", callErr, codes.Unimplemented, nil); err != nil {
		return err
	}
	if response != nil {
		return errors.New("unimplemented Range returned a response")
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	_, callErr = skeleton.Get(ctx, string(binaryKey), clientv3.WithRev(largeRevision))
	cancel()
	if err := requireFailure("skeleton clientv3 Get", callErr, codes.Unimplemented, nil); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	auth, callErr := pb.NewAuthClient(skeleton.ActiveConnection()).Authenticate(ctx, &pb.AuthenticateRequest{})
	cancel()
	if err := requireFailure("skeleton Authenticate", callErr, codes.Unimplemented, nil); err != nil {
		return err
	}
	if auth != nil {
		return errors.New("unimplemented Authenticate returned a token response")
	}
	if err := checkWatch(skeleton, timeout, codes.Unimplemented); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	keepalive, callErr := pb.NewLeaseClient(skeleton.ActiveConnection()).LeaseKeepAlive(ctx)
	if callErr == nil {
		callErr = keepalive.Send(&pb.LeaseKeepAliveRequest{ID: largeRevision})
	}
	if callErr == nil {
		_, callErr = keepalive.Recv()
	}
	cancel()
	if err := requireFailure("skeleton LeaseKeepAlive terminal", callErr, codes.Unimplemented, nil); err != nil {
		return err
	}

	// The Java integration test checks the received bytes and revision; the fixture only emits errors.
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	_, callErr = pb.NewKVClient(fixture.ActiveConnection()).Range(ctx, rangeRequest())
	cancel()
	if err := requireFailure("fixture bytes/int64 Range", callErr, codes.Unavailable, nil); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	_, callErr = fixture.Get(ctx, "permission")
	cancel()
	if err := requireFailure("fixture permission sentinel", callErr, codes.PermissionDenied, rpctypes.ErrPermissionDenied); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	_, callErr = fixture.Authenticate(ctx, "transport-test-user", "transport-test-password")
	cancel()
	if err := requireFailure("fixture authentication sentinel", callErr, codes.InvalidArgument, rpctypes.ErrAuthFailed); err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	_, callErr = pb.NewKVClient(fixture.ActiveConnection()).Range(ctx, &pb.RangeRequest{Key: []byte("timeout")})
	cancel()
	if err := requireFailure("fixture projected timeout", callErr, codes.DeadlineExceeded, nil); err != nil {
		return err
	}
	return checkWatch(fixture, min(timeout, time.Second), codes.DeadlineExceeded)
}

func checkWatch(client *clientv3.Client, timeout time.Duration, code codes.Code) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stream, err := pb.NewWatchClient(client.ActiveConnection()).Watch(ctx)
	if err == nil {
		err = stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{Key: binaryKey, RangeEnd: binaryEnd, StartRevision: largeRevision},
		}})
	}
	if err == nil {
		_, err = stream.Recv()
	}
	return requireFailure("Watch terminal", err, code, nil)
}

func rangeRequest() *pb.RangeRequest {
	return &pb.RangeRequest{Key: binaryKey, RangeEnd: binaryEnd, Revision: largeRevision, Limit: 1}
}

func requireFailure(name string, err error, expected codes.Code, sentinel error) error {
	code := status.Code(err)
	var codeProvider interface{ Code() codes.Code }
	if errors.As(err, &codeProvider) {
		code = codeProvider.Code()
	}
	if err == nil || code != expected {
		return fmt.Errorf("%s: got %s, want %s", name, code, expected)
	}
	if sentinel != nil && !errors.Is(err, sentinel) {
		return fmt.Errorf("%s: client did not recognize the expected etcd error", name)
	}
	return nil
}
