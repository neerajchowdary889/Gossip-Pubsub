package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"GossipPubsub/Publisher"
	"GossipPubsub/Subscriber"
)

const (
	defaultTopic    = "gossip-performance-test"
	defaultInterval = 1 * time.Second
)

// MessageStore holds received messages in a thread-safe hashtable
type MessageStore struct {
	mu       sync.RWMutex
	messages map[string]MessageEntry
}

type MessageEntry struct {
	Data      string
	Timestamp time.Time
	PeerID    string
	SeqNo     uint64
}

func NewMessageStore() *MessageStore {
	return &MessageStore{
		messages: make(map[string]MessageEntry),
	}
}

func (ms *MessageStore) Add(key string, data string, peerID string, seqNo uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages[key] = MessageEntry{
		Data:      data,
		Timestamp: time.Now(),
		PeerID:    peerID,
		SeqNo:     seqNo,
	}
}

func (ms *MessageStore) Print() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("MESSAGE STORE - Total: %d messages\n", len(ms.messages))
	fmt.Println(strings.Repeat("=", 80))

	if len(ms.messages) == 0 {
		fmt.Println("No messages received yet.")
		return
	}

	for key, entry := range ms.messages {
		fmt.Printf("\n[%s]\n", key)
		fmt.Printf("  Data:      %s\n", entry.Data)
		fmt.Printf("  From:      %s\n", entry.PeerID)
		fmt.Printf("  SeqNo:     %d\n", entry.SeqNo)
		fmt.Printf("  Received:  %s\n", entry.Timestamp.Format(time.RFC3339Nano))
		fmt.Printf("  Latency:   %v ago\n", time.Since(entry.Timestamp))
	}
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

func (ms *MessageStore) GetStats() map[string]interface{} {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	uniquePeers := make(map[string]bool)
	oldestMsg := time.Now()
	newestMsg := time.Time{}

	for _, entry := range ms.messages {
		uniquePeers[entry.PeerID] = true
		if entry.Timestamp.Before(oldestMsg) {
			oldestMsg = entry.Timestamp
		}
		if entry.Timestamp.After(newestMsg) {
			newestMsg = entry.Timestamp
		}
	}

	return map[string]interface{}{
		"total_messages": len(ms.messages),
		"unique_peers":   len(uniquePeers),
		"oldest_message": oldestMsg,
		"newest_message": newestMsg,
		"time_span":      newestMsg.Sub(oldestMsg),
	}
}

// CLI flags
var (
	mode         = flag.String("mode", "both", "Mode: publisher, subscriber, or both")
	port         = flag.Int("port", 0, "Port to listen on (0 for random)")
	bootstraps   = flag.String("bootstrap", "", "Comma-separated list of bootstrap peers (multiaddr format)")
	topicName    = flag.String("topic", defaultTopic, "PubSub topic name")
	publishInterval = flag.Duration("interval", defaultInterval, "Publishing interval (e.g., 1s, 500ms)")
	showAddrs    = flag.Bool("addrs", false, "Show node addresses and exit")
	printStore   = flag.Bool("print", false, "Print message store contents")
	keyFile      = flag.String("key", "", "Path to private key file (generates new if not exists)")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create libp2p host
	h, err := createHost(ctx, *port, *keyFile)
	if err != nil {
		log.Fatalf("Failed to create host: %v", err)
	}
	defer h.Close()

	log.Printf("🚀 Node created with ID: %s", h.ID())
	log.Printf("🔗 Multiaddresses: %v", h.Addrs())
	// Print addresses
	printHostAddrs(h)

	if *showAddrs {
		// Just show addresses and exit
		os.Exit(0)
	}

	// Connect to bootstrap peers if provided
	if *bootstraps != "" {
		connectToBootstraps(ctx, h, *bootstraps)
	}

	// Create PubSub instance
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("Failed to create PubSub: %v", err)
	}

	// Join topic
	topic, err := ps.Join(*topicName)
	if err != nil {
		log.Fatalf("Failed to join topic: %v", err)
	}
	defer topic.Close()

	log.Printf("📡 Joined topic: %s", *topicName)

	// Initialize message store
	msgStore := NewMessageStore()

	// Start components based on mode
	var wg sync.WaitGroup

	if *mode == "publisher" || *mode == "both" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Publisher.StartPublisher(ctx, topic, *publishInterval)
		}()
	}

	if *mode == "subscriber" || *mode == "both" {
		sub, err := topic.Subscribe()
		if err != nil {
			log.Fatalf("Failed to subscribe: %v", err)
		}
		defer sub.Cancel()

		wg.Add(1)
		go func() {
			defer wg.Done()
			startSubscriberWithStore(ctx, sub, msgStore)
		}()
	}

	// Print stats periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		printStatsPeriodically(ctx, h, msgStore)
	}()

	// Handle print command
	if *printStore {
		// Wait a bit for messages to arrive, then print and exit
		time.Sleep(5 * time.Second)
		msgStore.Print()
		cancel()
		wg.Wait()
		return
	}

	// Wait for shutdown signal
	log.Println("✅ Node running. Press Ctrl+C to stop.")
	<-sigCh
	log.Println("🛑 Shutting down...")
	
	// Print final stats
	msgStore.Print()
	
	cancel()
	wg.Wait()
	log.Println("👋 Goodbye!")
}

func createHost(ctx context.Context, port int, keyFile string) (host.Host, error) {
	var priv crypto.PrivKey
	var err error

	// Load or generate key
	if keyFile != "" {
		priv, err = loadOrGenerateKey(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load/generate key: %w", err)
		}
	} else {
		priv, _, err = crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to generate key: %w", err)
		}
	}

	// Create host options
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port)),
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip6/::/tcp/%d", port)),
		libp2p.NATPortMap(),
		libp2p.EnableRelay(),
	}

	return libp2p.New(opts...)
}

func loadOrGenerateKey(keyFile string) (crypto.PrivKey, error) {
	// Try to load existing key
	data, err := os.ReadFile(keyFile)
	if err == nil {
		return crypto.UnmarshalPrivateKey(data)
	}

	// Generate new key
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}

	// Save key
	data, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	if err := os.WriteFile(keyFile, data, 0600); err != nil {
		return nil, err
	}

	log.Printf("🔑 Generated new key and saved to: %s", keyFile)
	return priv, nil
}

func printHostAddrs(h host.Host) {
	fmt.Println("\n📍 Node Addresses:")
	for _, addr := range h.Addrs() {
		fmt.Printf("   %s/p2p/%s\n", addr, h.ID())
	}
	fmt.Println()
}

func connectToBootstraps(ctx context.Context, h host.Host, bootstrapStr string) {
	log.Printf("🔗 Connecting to bootstrap peers...")
	
	// Parse bootstrap addresses (comma-separated)
	// Format: /ip4/1.2.3.4/tcp/4001/p2p/QmPeerId
	addrs := parseMultiaddrs(bootstrapStr)
	
	for _, addr := range addrs {
		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Printf("⚠️  Invalid bootstrap address %s: %v", addr, err)
			continue
		}

		if err := h.Connect(ctx, *peerInfo); err != nil {
			log.Printf("⚠️  Failed to connect to %s: %v", peerInfo.ID, err)
		} else {
			log.Printf("✅ Connected to bootstrap peer: %s", peerInfo.ID)
		}
	}
}

func parseMultiaddrs(addrsStr string) []multiaddr.Multiaddr {
	var result []multiaddr.Multiaddr
	
	// Split by comma
	parts := splitAndTrim(addrsStr, ",")
	
	for _, part := range parts {
		addr, err := multiaddr.NewMultiaddr(part)
		if err != nil {
			log.Printf("⚠️  Invalid multiaddr '%s': %v", part, err)
			continue
		}
		result = append(result, addr)
	}
	
	return result
}

func splitAndTrim(s, sep string) []string {
	var result []string
	for _, part := range split(s, sep) {
		trimmed := trim(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func split(s, sep string) []string {
	// Simple string split implementation
	var result []string
	start := 0
	
	for i := 0; i < len(s); i++ {
		if i+len(sep) <= len(s) && s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n') {
		end--
	}
	
	return s[start:end]
}

func startSubscriberWithStore(ctx context.Context, sub *pubsub.Subscription, store *MessageStore) {
	subscriber := Subscriber.NewSubscriber(sub)
	
	log.Printf("📨 Subscriber started with message store...")

	for {
		select {
		case <-ctx.Done():
			log.Printf("📨 Subscriber stopping...")
			return
		default:
			// Use custom handler to store messages
			err := subscriber.ProcessMessageWithHandler(ctx, func(msg *pubsub.Message) error {
				// Generate unique key for hashtable
				key := fmt.Sprintf("%s-%d", msg.ReceivedFrom.String()[:8], msg.GetSeqno())

				// Convert seqno from []byte to uint64 for storage
				var seq uint64
				for i := 0; i < len(msg.GetSeqno()) && i < 8; i++ {
					seq = (seq << 8) | uint64(msg.GetSeqno()[i])
				}

				// Store in hashtable
				store.Add(key, string(msg.Data), msg.ReceivedFrom.String(), seq)

				// Log receipt
				log.Printf("📨 Received [%s]: %s", key, string(msg.Data))
				return nil

			})

			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️  Subscriber error: %v", err)
				time.Sleep(time.Second)
			}
		}
	}
}

func printStatsPeriodically(ctx context.Context, h host.Host, store *MessageStore) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printStats(h, store)
		}
	}
}

func printStats(h host.Host, store *MessageStore) {
	stats := store.GetStats()
	peers := h.Network().Peers()

	fmt.Println("\n" + strings.Repeat("─", 80))
	fmt.Println("📊 PERFORMANCE STATS")
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("Connected Peers:    %d\n", len(peers))
	fmt.Printf("Messages Received:  %d\n", stats["total_messages"])
	fmt.Printf("Unique Senders:     %d\n", stats["unique_peers"])
	
	if stats["total_messages"].(int) > 0 {
		fmt.Printf("Time Span:          %v\n", stats["time_span"])
		fmt.Printf("Oldest Message:     %v\n", stats["oldest_message"].(time.Time).Format(time.RFC3339))
		fmt.Printf("Newest Message:     %v\n", stats["newest_message"].(time.Time).Format(time.RFC3339))
	}
	
	fmt.Println(strings.Repeat("─", 80) + "\n")
}