package client

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Options struct {
	Endpoint string
	Timeout  time.Duration
}

func (options Options) Validate() error {
	if options.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if options.Timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	return validateEndpoint(options.Endpoint)
}

func validateEndpoint(endpoint string) error {
	if len(endpoint) > 4096 {
		return errors.New("endpoint is too long")
	}
	hostPort := endpoint
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("endpoint must not contain credentials, a path, or query data")
		}
		hostPort = parsed.Host
	}
	host, portText, err := net.SplitHostPort(hostPort)
	if err != nil || host == "" || strings.ContainsAny(host, "@/?#% \t\r\n") {
		return errors.New("endpoint requires an explicit host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return errors.New("endpoint port is invalid")
	}
	return nil
}

func New(options Options) (*clientv3.Client, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{options.Endpoint},
		DialTimeout: options.Timeout,
		Logger:      zap.NewNop(),
	})
}

func WithDeadline(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, errors.New("parent context is required")
	}
	if timeout <= 0 {
		return nil, nil, errors.New("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}

func RawKV(client *clientv3.Client) etcdserverpb.KVClient {
	return etcdserverpb.NewKVClient(client.ActiveConnection())
}

func RawWatch(client *clientv3.Client) etcdserverpb.WatchClient {
	return etcdserverpb.NewWatchClient(client.ActiveConnection())
}
