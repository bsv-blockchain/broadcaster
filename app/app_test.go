package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/bsv-blockchain/broadcaster/config"
	"github.com/bsv-blockchain/broadcaster/kafka"
)

func TestModeNeedsChaintracks(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"all", true},
		{"chaintracks", true},
		{"bump-builder", true},
		{"api-server", false},
		{"sse", false},
		{"propagation", false},
		{"p2p-client", false},
		{"watchdog", false},
		{"", false},
		{"unknown-mode", false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			if got := modeNeedsChaintracks(tc.mode); got != tc.want {
				t.Errorf("modeNeedsChaintracks(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

func TestModeNeedsTxTracker(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"all", true},
		{"api-server", true},
		{"bump-builder", false},
		{"sse", false},
		{"watchdog", false},
		{"propagation", false},
		{"p2p-client", false},
		{"chaintracks", false},
		{"webhook", false},
		{"", false},
		{"unknown-mode", false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			if got := modeNeedsTxTracker(tc.mode); got != tc.want {
				t.Errorf("modeNeedsTxTracker(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

func TestModeNeedsValidator(t *testing.T) {
	cases := []struct {
		mode string
		want bool
	}{
		{"all", true},
		{"api-server", true},
		{"bump-builder", false},
		{"sse", false},
		{"watchdog", false},
		{"propagation", false},
		{"p2p-client", false},
		{"chaintracks", false},
		{"webhook", false},
		{"", false},
		{"unknown-mode", false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			if got := modeNeedsValidator(tc.mode); got != tc.want {
				t.Errorf("modeNeedsValidator(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

func TestValidatorPolicyFromConfig(t *testing.T) {
	// The policy is always non-nil now that the size/sigop fields thread
	// through (issue #212); wantFeeNil asserts only the MinFeePerKB pointer,
	// which drives the validator's DefaultMinFeePerKB fallback when nil.
	cases := []struct {
		name        string
		acceptZero  bool
		minFeePerKB uint64
		wantFeeNil  bool
		wantFee     uint64
	}{
		{
			name:       "unset fee leaves MinFeePerKB nil so NewValidator applies its default",
			wantFeeNil: true,
		},
		{
			name:        "explicit min_fee_per_kb threads through",
			minFeePerKB: 50,
			wantFee:     50,
		},
		{
			name:        "min_fee_per_kb=0 without flag leaves MinFeePerKB nil (default floor)",
			minFeePerKB: 0,
			wantFeeNil:  true,
		},
		{
			name:       "accept_zero_fee pins fee to zero",
			acceptZero: true,
			wantFee:    0,
		},
		{
			name:        "accept_zero_fee wins over min_fee_per_kb",
			acceptZero:  true,
			minFeePerKB: 500,
			wantFee:     0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Validator.AcceptZeroFee = tc.acceptZero
			cfg.Validator.MinFeePerKB = tc.minFeePerKB

			got := validatorPolicyFromConfig(cfg)
			if got == nil {
				t.Fatalf("expected non-nil policy, got nil")
			}
			if tc.wantFeeNil {
				if got.MinFeePerKB != nil {
					t.Fatalf("expected nil MinFeePerKB, got %d", *got.MinFeePerKB)
				}
				return
			}
			if got.MinFeePerKB == nil {
				t.Fatalf("expected non-nil MinFeePerKB, got nil")
			}
			if *got.MinFeePerKB != tc.wantFee {
				t.Errorf("MinFeePerKB = %d, want %d", *got.MinFeePerKB, tc.wantFee)
			}
		})
	}
}

func TestResolveChaintracksBootstrapPeers(t *testing.T) {
	const sharedPeer = "/dns4/shared.example/tcp/9905/p2p/12D3KooWShared"
	const ctSpecific = "/dns4/chaintracks.example/tcp/9905/p2p/12D3KooWCT"

	cases := []struct {
		name             string
		network          string
		p2pPeers         []string
		chaintracksPeers []string
		want             []string
	}{
		{
			name:    "testnet default when nothing set",
			network: config.NetworkTestnet,
			want:    []string{"/dnsaddr/testnet.bootstrap.teranode.bsvb.tech"},
		},
		{
			name:    "regtest default is nil when nothing set",
			network: config.NetworkRegtest,
			want:    nil,
		},
		{
			name:     "p2p.bootstrap_peers feeds chaintracks",
			network:  config.NetworkRegtest,
			p2pPeers: []string{sharedPeer},
			want:     []string{sharedPeer},
		},
		{
			name:             "chaintracks-specific wins over p2p",
			network:          config.NetworkRegtest,
			p2pPeers:         []string{sharedPeer},
			chaintracksPeers: []string{ctSpecific},
			want:             []string{ctSpecific},
		},
		{
			name:             "chaintracks-specific wins over network default",
			network:          config.NetworkTestnet,
			chaintracksPeers: []string{ctSpecific},
			want:             []string{ctSpecific},
		},
		{
			name:     "p2p list wins over network default",
			network:  config.NetworkMainnet,
			p2pPeers: []string{sharedPeer},
			want:     []string{sharedPeer},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{Network: tc.network}
			cfg.P2P.BootstrapPeers = tc.p2pPeers
			cfg.Chaintracks.P2P.MsgBus.BootstrapPeers = tc.chaintracksPeers

			got := resolveChaintracksBootstrapPeers(cfg)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("resolveChaintracksBootstrapPeers() = %v, want %v", got, tc.want)
			}
		})
	}
}

// partitionProbe records every PartitionCount call. width is returned for
// topics that are not listed in missing; those return ErrTopicNotFound.
type partitionProbe struct {
	width   int
	missing map[string]bool
	seen    []string
}

func (p *partitionProbe) Send(context.Context, string, string, []byte) error { return nil }

func (p *partitionProbe) SendAsync(context.Context, string, string, []byte) error { return nil }

func (p *partitionProbe) SendBatch(context.Context, string, []kafka.KeyValue) error { return nil }

func (p *partitionProbe) Subscribe(string, []string, kafka.StartOffset) (kafka.Subscription, error) {
	return nil, errors.New("partitionProbe: Subscribe not used")
}

func (p *partitionProbe) Close() error { return nil }

func (p *partitionProbe) PartitionCount(topic string) (int, error) {
	p.seen = append(p.seen, topic)
	if p.missing[topic] {
		return 0, kafka.ErrTopicNotFound
	}
	return p.width, nil
}

func TestPartitionPreflight_ChecksHorizontalScaleTopics(t *testing.T) {
	kafka.SetTopicPrefix("bsv-broadcaster")
	t.Cleanup(func() { kafka.SetTopicPrefix(kafka.DefaultTopicPrefix) })

	probe := &partitionProbe{width: 4}
	cfg := &config.Config{}
	cfg.Propagation.Partitions = 2
	cfg.Kafka.MinPartitions = 3
	if err := partitionPreflight(probe, cfg, zap.NewNop()); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	want := append([]string{kafka.TopicPropagation}, kafka.HorizontalScaleTopics()...)
	if !reflect.DeepEqual(probe.seen, want) {
		t.Fatalf("checked topics %v, want %v", probe.seen, want)
	}
}

func TestPartitionPreflight_MissingScaleTopicFails(t *testing.T) {
	kafka.SetTopicPrefix("bsv-broadcaster")
	t.Cleanup(func() { kafka.SetTopicPrefix(kafka.DefaultTopicPrefix) })

	probe := &partitionProbe{
		width:   4,
		missing: map[string]bool{kafka.TopicBlockProcessed: true},
	}
	cfg := &config.Config{}
	cfg.Propagation.Partitions = 1
	cfg.Kafka.MinPartitions = 3
	err := partitionPreflight(probe, cfg, zap.NewNop())
	if !errors.Is(err, kafka.ErrTopicNotFound) {
		t.Fatalf("error %v should wrap ErrTopicNotFound", err)
	}
	if !strings.Contains(err.Error(), kafka.TopicBlockProcessed) {
		t.Fatalf("error %q should name the missing fan-out topic", err)
	}
	for _, topic := range probe.seen {
		if topic == kafka.TopicStatusUpdate {
			t.Fatal("preflight continued past the missing topic")
		}
	}
}

func TestPartitionPreflight_FloorAtMostOneSkipsScaleTopics(t *testing.T) {
	kafka.SetTopicPrefix("bsv-broadcaster")
	t.Cleanup(func() { kafka.SetTopicPrefix(kafka.DefaultTopicPrefix) })

	probe := &partitionProbe{width: 1}
	cfg := &config.Config{}
	cfg.Propagation.Partitions = 1
	cfg.Kafka.MinPartitions = 1
	if err := partitionPreflight(probe, cfg, zap.NewNop()); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if !reflect.DeepEqual(probe.seen, []string{kafka.TopicPropagation}) {
		t.Fatalf("checked topics %v, want only propagation", probe.seen)
	}
}

func TestBootstrap_MinPartitionsFailsOnNarrowHotPathTopic(t *testing.T) {
	cfg := &config.Config{}
	cfg.Kafka.Backend = "memory"
	cfg.Kafka.MinPartitions = 3
	cfg.Propagation.Partitions = 1

	_, cleanup, err := Bootstrap(context.Background(), cfg, zap.NewNop())
	if cleanup != nil {
		defer cleanup()
	}
	if err == nil {
		t.Fatal("expected startup to fail the partition preflight")
	}
	if !strings.Contains(err.Error(), "kafka partition check") {
		t.Fatalf("error %q should come from the partition preflight", err)
	}
	if !strings.Contains(err.Error(), kafka.TopicBlockProcessed) {
		t.Fatalf("error %q should name %s", err, kafka.TopicBlockProcessed)
	}
}
