package CRDTSync

import (
	"context"
	"time"

	CRDT "GossipPubsub/CRDT"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

// SyncMode represents the synchronization mode
type SyncMode string

const (
	ModePublish   SyncMode = "publish"
	ModeSubscribe SyncMode = "subscribe"
	ModeBoth      SyncMode = "both"
)

// Config holds configuration for CRDT synchronization
type Config struct {
	Mode              SyncMode
	SyncInterval      time.Duration
	TopicName         string
	PrintStore        bool
	HeartbeatInterval time.Duration
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Mode:              ModeBoth,
		SyncInterval:      7 * time.Second,
		TopicName:         "crdt-topic",
		PrintStore:        false,
		HeartbeatInterval: 30 * time.Second,
	}
}

// SyncEngine defines the interface for CRDT synchronization
type SyncEngine interface {
	// Start begins the synchronization process
	Start(ctx context.Context) error

	// Stop gracefully stops the synchronization
	Stop() error

	// SyncNow forces an immediate synchronization
	SyncNow() error

	// GetStats returns synchronization statistics
	GetStats() map[string]interface{}

	// GetMessageStore returns the message store
	GetMessageStore() *MessageStore

	// AddLocalData adds local CRDT data
	AddLocalData(counter uint64) error

	// GetCRDTEngine returns the underlying CRDT engine
	GetCRDTEngine() *CRDT.Engine
}

// Node represents a CRDT-enabled node with synchronization capabilities
type Node struct {
	nodeID          string
	host            host.Host
	engine          *CRDT.Engine
	topic           *pubsub.Topic
	sub             *pubsub.Subscription
	msgStore        *MessageStore
	config          *Config
	ctx             context.Context
	cancel          context.CancelFunc
	syncTicker      *time.Ticker
	heartbeatTicker *time.Ticker
}

// NewNode creates a new CRDT synchronization node
func NewNode(
	host host.Host,
	engine *CRDT.Engine,
	topic *pubsub.Topic,
	config *Config,
) *Node {
	ctx, cancel := context.WithCancel(context.Background())

	return &Node{
		nodeID:   host.ID().String(),
		host:     host,
		engine:   engine,
		topic:    topic,
		msgStore: NewMessageStore(),
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Ensure Node implements SyncEngine interface
var _ SyncEngine = (*Node)(nil)
