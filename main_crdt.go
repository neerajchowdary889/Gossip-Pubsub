package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"

	CRDT "GossipPubsub/CRDT"
	"GossipPubsub/Peer"
)

// Constants are defined in main.go

// CRDTMessage represents a message sent over PubSub for CRDT synchronization
type CRDTMessage struct {
	Type      string                     `json:"type"` // "operation" or "sync"
	NodeID    string                     `json:"node_id"`
	Key       string                     `json:"key"`
	Operation *CRDTOperation             `json:"operation,omitempty"`
	SyncData  map[string]json.RawMessage `json:"sync_data,omitempty"`
	Timestamp time.Time                  `json:"timestamp"`
}

// CRDTOperation represents a CRDT operation
type CRDTOperation struct {
	Kind    string           `json:"kind"` // "add", "remove", "increment"
	Element string           `json:"element,omitempty"`
	Value   uint64           `json:"value,omitempty"`
	TS      CRDT.VectorClock `json:"timestamp"`
}

// CRDTNode represents a CRDT-enabled node
type CRDTNode struct {
	nodeID   string
	host     host.Host
	engine   *CRDT.Engine
	topic    *pubsub.Topic
	sub      *pubsub.Subscription
	msgStore *CRDTMessageStore
	ctx      context.Context
	cancel   context.CancelFunc
}

// CRDTMessageStore holds received CRDT messages
type CRDTMessageStore struct {
	mu       sync.RWMutex
	messages map[string]CRDTMessage
}

func NewCRDTMessageStore() *CRDTMessageStore {
	return &CRDTMessageStore{
		messages: make(map[string]CRDTMessage),
	}
}

func (ms *CRDTMessageStore) Add(key string, msg CRDTMessage) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages[key] = msg
}

func (ms *CRDTMessageStore) Print() {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("CRDT MESSAGE STORE - Total: %d messages\n", len(ms.messages))
	fmt.Println(strings.Repeat("=", 80))

	if len(ms.messages) == 0 {
		fmt.Println("No CRDT messages received yet.")
		return
	}

	for key, msg := range ms.messages {
		fmt.Printf("\n[%s]\n", key)
		fmt.Printf("  Type:      %s\n", msg.Type)
		fmt.Printf("  From:      %s\n", msg.NodeID)
		fmt.Printf("  Key:       %s\n", msg.Key)
		fmt.Printf("  Timestamp: %s\n", msg.Timestamp.Format(time.RFC3339Nano))

		if msg.Operation != nil {
			fmt.Printf("  Operation: %s", msg.Operation.Kind)
			if msg.Operation.Element != "" {
				fmt.Printf(" element=%s", msg.Operation.Element)
			}
			if msg.Operation.Value > 0 {
				fmt.Printf(" value=%d", msg.Operation.Value)
			}
			fmt.Println()
		}
	}
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

func crdtMain() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create libp2p host
	h, err := createCRDTHost(ctx, *CRDTPort)
	if err != nil {
		log.Fatalf("Failed to create CRDT host: %v", err)
	}
	defer h.Close()

	log.Printf("🚀 CRDT Node created with ID: %s", h.ID())
	log.Printf("🔗 CRDT Multiaddresses: %v", h.Addrs())

	// Print addresses for manual connection (reuse from main.go)
	printHostAddrs(h)

	// Load and display the PeerID from Peer package
	if peerID, err := Peer.LoadPeerID(); err == nil {
		log.Printf("📋 CRDT PeerID from Peer package: %s", peerID)
	} else {
		log.Printf("⚠️  Could not load PeerID from Peer package: %v", err)
	}

	// Connect to bootstrap peers if provided (reuse from main.go)
	if *bootstraps != "" {
		connectToBootstraps(ctx, h, *bootstraps)
	}

	// Create CRDT engine
	engine := CRDT.NewEngineMemOnly(50 * 1024 * 1024) // 50MB heap
	log.Printf("📊 CRDT Engine initialized with 50MB heap")

	// Create PubSub instance
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatalf("Failed to create CRDT PubSub: %v", err)
	}

	// Join CRDT topic
	topic, err := ps.Join(*CRDTTopic)
	if err != nil {
		log.Fatalf("Failed to join CRDT topic: %v", err)
	}
	defer topic.Close()

	log.Printf("📡 Joined CRDT topic: %s", *CRDTTopic)

	// Wait a moment for peer connections to establish
	time.Sleep(2 * time.Second)

	// Check peer connections
	peers := h.Network().Peers()
	log.Printf("🔗 Connected peers: %d", len(peers))
	for _, peerID := range peers {
		log.Printf("   - %s", peerID)
	}

	// Create CRDT node
	node := &CRDTNode{
		nodeID:   h.ID().String(),
		host:     h,
		engine:   engine,
		topic:    topic,
		msgStore: NewCRDTMessageStore(),
		ctx:      ctx,
		cancel:   cancel,
	}

	// Start components based on mode
	// Every node is both a publisher and a subscriber
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		node.startCRDTPublisher()
	}()

	sub, err := topic.Subscribe()
	if err != nil {
		log.Fatalf("Failed to subscribe to CRDT topic: %v", err)
	}
	defer sub.Cancel()

	node.sub = sub
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.startCRDTSubscriber()
	}()

	// Print CRDT stats periodically
	wg.Add(1)
	go func() {
		defer wg.Done()
		node.printCRDTStatsPeriodically()
	}()

	// Handle print command
	if *CRDTPrintStore {
		// Wait a bit for messages to arrive, then print and exit
		time.Sleep(5 * time.Second)
		node.printCRDTState()
		cancel()
		wg.Wait()
		return
	}

	// Wait for shutdown signal
	log.Println("✅ CRDT Node running. Press Ctrl+C to stop.")

	// Use a select to handle both signals and context cancellation
	select {
	case <-sigCh:
		log.Println("🛑 Received shutdown signal...")
	case <-ctx.Done():
		log.Println("🛑 Context cancelled...")
	}

	log.Println("🛑 Shutting down CRDT node...")

	// Print final CRDT state
	node.printCRDTState()

	// Cancel context to stop all goroutines
	cancel()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("👋 All goroutines stopped gracefully")
	case <-time.After(5 * time.Second):
		log.Println("⚠️  Timeout waiting for goroutines to stop")
		log.Println("🔄 Force exiting...")
		os.Exit(0)
	}

	log.Println("👋 CRDT Node goodbye!")
}

// createCRDTHost reuses the existing createHost function from main.go
func createCRDTHost(ctx context.Context, port int) (host.Host, error) {
	return createHost(ctx, port, "")
}

func (n *CRDTNode) startCRDTPublisher() {
	ticker := time.NewTicker(*CRDTInterval)
	defer ticker.Stop()

	counter := uint64(0)
	log.Printf("📤 CRDT Publisher started, syncing every %v", *CRDTInterval)

	for {
		select {
		case <-n.ctx.Done():
			log.Printf("📤 CRDT Publisher stopping...")
			return
		case <-ticker.C:
			// Add local data first
			n.addLocalCRDTData(counter)

			// Then sync the entire CRDT state
			if err := n.syncCRDTState(); err != nil {
				log.Printf("❌ CRDT Sync error: %v", err)
			} else {
				counter++
			}
		}
	}
}

func (n *CRDTNode) addLocalCRDTData(counter uint64) {
	// Add data to local CRDT sets and counters
	nodeID := n.nodeID

	// Add to sample set
	element := fmt.Sprintf("node-%s-element-%d", nodeID[:8], counter)
	ts := CRDT.VectorClock{nodeID: counter + 1}

	if err := n.engine.LWWAdd(nodeID, "sample-set", element, ts); err != nil {
		log.Printf("⚠️  Failed to add to local set: %v", err)
	} else {
		log.Printf("📝 Added local element: %s", element)
	}

	// Increment counter
	if err := n.engine.CounterInc(nodeID, "sample-counter", 1, ts); err != nil {
		log.Printf("⚠️  Failed to increment local counter: %v", err)
	} else {
		log.Printf("📝 Incremented local counter")
	}
}

func (n *CRDTNode) syncCRDTState() error {
	// Get all CRDTs from local engine
	allCRDTs := n.engine.GetAllCRDTs()

	// Convert CRDTs to JSON RawMessage for proper serialization
	syncData := make(map[string]json.RawMessage)
	for key, crdt := range allCRDTs {
		// Create a wrapper with type information
		var wrapper struct {
			Type string      `json:"type"`
			Data interface{} `json:"data"`
		}

		switch crdt.(type) {
		case *CRDT.LWWSet:
			wrapper.Type = "lwwset"
			wrapper.Data = crdt
		case *CRDT.Counter:
			wrapper.Type = "counter"
			wrapper.Data = crdt
		default:
			log.Printf("⚠️  Unknown CRDT type for key %s", key)
			continue
		}

		data, err := json.Marshal(wrapper)
		if err != nil {
			log.Printf("⚠️  Failed to marshal CRDT %s: %v", key, err)
			continue
		}
		syncData[key] = data
	}

	// Create sync message
	msg := CRDTMessage{
		Type:      "sync",
		NodeID:    n.nodeID,
		Key:       "all-crdts",
		SyncData:  syncData,
		Timestamp: time.Now(),
	}

	// Serialize and publish
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal CRDT sync message: %w", err)
	}

	if err := n.topic.Publish(n.ctx, data); err != nil {
		return fmt.Errorf("failed to publish CRDT sync message: %w", err)
	}

	log.Printf("📤 Synced CRDT state: %d objects", len(allCRDTs))
	return nil
}

func (n *CRDTNode) startCRDTSubscriber() {
	log.Printf("📨 CRDT Subscriber started, listening for messages...")

	for {
		select {
		case <-n.ctx.Done():
			log.Printf("📨 CRDT Subscriber stopping...")
			return
		default:
			if err := n.receiveCRDTMessage(); err != nil {
				if n.ctx.Err() != nil {
					return
				}
				log.Printf("❌ CRDT Subscription error: %v", err)
				time.Sleep(time.Second)
			}
		}
	}
}

func (n *CRDTNode) receiveCRDTMessage() error {
	// Create a context with timeout to avoid blocking indefinitely
	ctx, cancel := context.WithTimeout(n.ctx, 1*time.Second)
	defer cancel()

	msg, err := n.sub.Next(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err() // Return context error for proper handling
		}
		return fmt.Errorf("failed to receive CRDT message: %w", err)
	}

	// Parse CRDT message
	var crdtMsg CRDTMessage
	if err := json.Unmarshal(msg.Data, &crdtMsg); err != nil {
		return fmt.Errorf("failed to unmarshal CRDT message: %w", err)
	}

	// Skip our own messages
	if crdtMsg.NodeID == n.nodeID {
		return nil
	}

	// Store message
	key := fmt.Sprintf("%s-%d", crdtMsg.NodeID[:8], crdtMsg.Timestamp.UnixNano())
	n.msgStore.Add(key, crdtMsg)

	// Handle sync messages
	if crdtMsg.Type == "sync" && crdtMsg.SyncData != nil {
		if err := n.applyCRDTSync(crdtMsg); err != nil {
			log.Printf("⚠️  Failed to apply CRDT sync: %v", err)
		}
	}

	log.Printf("📨 Received CRDT message from %s: %s", crdtMsg.NodeID[:8], crdtMsg.Type)
	return nil
}

func (n *CRDTNode) applyCRDTSync(msg CRDTMessage) error {
	log.Printf("🔄 Applying CRDT sync from %s with %d objects", msg.NodeID[:8], len(msg.SyncData))

	// Get our current CRDTs
	ourCRDTs := n.engine.GetAllCRDTs()

	// Merge each CRDT from the sync message
	for key, rawData := range msg.SyncData {
		// Unmarshal the wrapper with type information
		var wrapper struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(rawData, &wrapper); err != nil {
			log.Printf("⚠️  Failed to unmarshal CRDT wrapper %s: %v", key, err)
			continue
		}

		// Unmarshal based on type
		var remoteCRDT CRDT.CRDT
		switch wrapper.Type {
		case "counter":
			var counter CRDT.Counter
			if err := json.Unmarshal(wrapper.Data, &counter); err != nil {
				log.Printf("⚠️  Failed to unmarshal Counter %s: %v", key, err)
				continue
			}
			remoteCRDT = &counter
		case "lwwset":
			var lwwSet CRDT.LWWSet
			if err := json.Unmarshal(wrapper.Data, &lwwSet); err != nil {
				log.Printf("⚠️  Failed to unmarshal LWWSet %s: %v", key, err)
				continue
			}
			remoteCRDT = &lwwSet
		default:
			log.Printf("⚠️  Unknown CRDT type %s for key %s", wrapper.Type, key)
			continue
		}

		if ourCRDT, exists := ourCRDTs[key]; exists {
			// Both nodes have this CRDT - merge them
			mergedCRDT, err := ourCRDT.Merge(remoteCRDT)
			if err != nil {
				log.Printf("⚠️  Failed to merge CRDT %s: %v", key, err)
				continue
			}

			// Apply the merged result
			n.engine.ApplyMergedCRDT(key, mergedCRDT)
			log.Printf("✅ Merged CRDT %s", key)
		} else {
			// We don't have this CRDT - just apply it
			n.engine.ApplyMergedCRDT(key, remoteCRDT)
			log.Printf("✅ Applied new CRDT %s", key)
		}
	}

	return nil
}

func (n *CRDTNode) printCRDTStatsPeriodically() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.printCRDTStats()
		}
	}
}

func (n *CRDTNode) printCRDTStats() {
	peers := n.host.Network().Peers()

	fmt.Println("\n" + strings.Repeat("─", 80))
	fmt.Println("📊 CRDT PERFORMANCE STATS")
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("Connected Peers:    %d\n", len(peers))

	// Get CRDT state
	allCRDTs := n.engine.GetAllCRDTs()
	fmt.Printf("CRDT Objects:       %d\n", len(allCRDTs))

	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			fmt.Printf("  Set '%s':         %d elements\n", key, len(elements))
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			fmt.Printf("  Counter '%s':     %d\n", key, value)
		}
	}

	fmt.Println(strings.Repeat("─", 80) + "\n")
}

func (n *CRDTNode) printCRDTState() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL CRDT STATE")
	fmt.Println(strings.Repeat("=", 80))

	// Print CRDT objects
	allCRDTs := n.engine.GetAllCRDTs()
	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			fmt.Printf("\nSet '%s':\n", key)
			for i, element := range elements {
				fmt.Printf("  [%d] %s\n", i+1, element)
			}
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			fmt.Printf("\nCounter '%s': %d\n", key, value)
		}
	}

	// Print message store
	n.msgStore.Print()
	fmt.Println(strings.Repeat("=", 80) + "\n")
}
