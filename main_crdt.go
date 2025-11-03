package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"

	CRDT "GossipPubsub/CRDT"
	"GossipPubsub/CRDTSync"
	"GossipPubsub/Peer"
)

// Constants are defined in main.go

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

	// Start mDNS discovery for auto-discovery
	mdnsService := mdns.NewMdnsService(h, "gossip-crdt", nil)
	mdnsService.Start()
	log.Printf("🔍 mDNS discovery started for auto-discovery")

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
	log.Printf("🎯 CRDT Mode: %s", *CRDTMode)

	// Wait a moment for peer connections to establish
	time.Sleep(2 * time.Second)

	// Check peer connections
	peers := h.Network().Peers()
	log.Printf("🔗 Connected peers: %d", len(peers))
	for _, peerID := range peers {
		log.Printf("   - %s", peerID)
	}

	// Create CRDTSync configuration
	config := &CRDTSync.Config{
		Mode:              CRDTSync.SyncMode(*CRDTMode),
		SyncInterval:      *CRDTInterval,
		TopicName:         *CRDTTopic,
		PrintStore:        *CRDTPrintStore,
		HeartbeatInterval: 30 * time.Second,
	}

	// Create CRDT sync node
	syncNode := CRDTSync.NewNode(h, engine, topic, config)

	// Start the sync node
	if err := syncNode.Start(ctx); err != nil {
		log.Fatalf("Failed to start CRDT sync: %v", err)
	}

	// Start stats printer
	syncNode.StartStatsPrinter(30 * time.Second)

	// Handle print command
	if *CRDTPrintStore {
		// Wait a bit for messages to arrive, then print and exit
		time.Sleep(5 * time.Second)
		syncNode.PrintFinalState()
		cancel()
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
	syncNode.PrintFinalState()

	// Stop the sync node
	if err := syncNode.Stop(); err != nil {
		log.Printf("⚠️  Error stopping sync node: %v", err)
	}

	log.Println("👋 CRDT Node goodbye!")
}

// createCRDTHost creates a unique host for CRDT nodes based on port
func createCRDTHost(ctx context.Context, port int) (host.Host, error) {
	// Generate a unique key file for each CRDT node based on port
	return createHost(ctx, port, "")
}
