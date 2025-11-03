package CRDTSync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	CRDT "GossipPubsub/CRDT"
)

// Start begins the synchronization process
func (n *Node) Start(ctx context.Context) error {
	// Subscribe to topic if in subscribe or both mode
	if n.config.Mode == ModeSubscribe || n.config.Mode == ModeBoth {
		sub, err := n.topic.Subscribe()
		if err != nil {
			return fmt.Errorf("failed to subscribe to topic: %w", err)
		}
		n.sub = sub

		// Start subscriber goroutine
		go n.startSubscriber()
	}

	// Start publisher if in publish or both mode
	if n.config.Mode == ModePublish || n.config.Mode == ModeBoth {
		n.syncTicker = time.NewTicker(n.config.SyncInterval)
		go n.startPublisher()
	}

	// Start heartbeat if subscriber
	if n.config.Mode == ModeSubscribe || n.config.Mode == ModeBoth {
		n.heartbeatTicker = time.NewTicker(n.config.HeartbeatInterval)
		go n.startHeartbeat()
	}

	// Send initial sync request if subscriber
	if n.config.Mode == ModeSubscribe || n.config.Mode == ModeBoth {
		if err := n.sendSyncRequest(); err != nil {
			log.Printf("⚠️  Failed to send initial sync request: %v", err)
		}
	}

	return nil
}

// Stop gracefully stops the synchronization
func (n *Node) Stop() error {
	n.cancel()

	if n.syncTicker != nil {
		n.syncTicker.Stop()
	}

	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
	}

	if n.sub != nil {
		n.sub.Cancel()
	}

	return nil
}

// SyncNow forces an immediate synchronization
func (n *Node) SyncNow() error {
	if n.config.Mode == ModePublish || n.config.Mode == ModeBoth {
		return n.syncCRDTState()
	}
	return fmt.Errorf("node is not in publish mode")
}

// GetStats returns synchronization statistics
func (n *Node) GetStats() map[string]interface{} {
	peers := n.host.Network().Peers()
	allCRDTs := n.engine.GetAllCRDTs()

	stats := map[string]interface{}{
		"connected_peers":   len(peers),
		"crdt_objects":      len(allCRDTs),
		"messages_received": n.msgStore.Count(),
		"mode":              string(n.config.Mode),
		"sync_interval":     n.config.SyncInterval.String(),
	}

	// Add CRDT-specific stats
	crdtStats := make(map[string]interface{})
	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			crdtStats[key] = map[string]interface{}{
				"type":     "lwwset",
				"elements": len(elements),
			}
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			crdtStats[key] = map[string]interface{}{
				"type":  "counter",
				"value": value,
			}
		}
	}
	stats["crdt_details"] = crdtStats

	return stats
}

// GetMessageStore returns the message store
func (n *Node) GetMessageStore() *MessageStore {
	return n.msgStore
}

// GetCRDTEngine returns the underlying CRDT engine
func (n *Node) GetCRDTEngine() *CRDT.Engine {
	return n.engine
}

// AddLocalData adds local CRDT data
func (n *Node) AddLocalData(counter uint64) error {
	nodeID := n.nodeID
	peerID := nodeID[len(nodeID)-6:] // Last 6 letters of peer ID

	// Add to sample set
	element := fmt.Sprintf("%s-%d", peerID, counter)
	ts := CRDT.VectorClock{nodeID: counter + 1}

	if err := n.engine.LWWAdd(nodeID, "sample-set", element, ts); err != nil {
		return fmt.Errorf("failed to add to local set: %w", err)
	}

	// Increment peer-specific counter
	counterKey := fmt.Sprintf("%s-counter", peerID)
	if err := n.engine.CounterInc(nodeID, counterKey, 1, ts); err != nil {
		return fmt.Errorf("failed to increment local counter: %w", err)
	}

	return nil
}

// startPublisher starts the publisher goroutine
func (n *Node) startPublisher() {
	counter := uint64(0)
	log.Printf("📤 CRDT Publisher started, syncing every %v", n.config.SyncInterval)

	for {
		select {
		case <-n.ctx.Done():
			log.Printf("📤 CRDT Publisher stopping...")
			return
		case <-n.syncTicker.C:
			// Add local data first
			if err := n.AddLocalData(counter); err != nil {
				log.Printf("⚠️  Failed to add local data: %v", err)
			} else {
				log.Printf("📝 Added local element: %s-%d", n.nodeID[len(n.nodeID)-6:], counter)
				log.Printf("📝 Incremented local counter: %s-counter", n.nodeID[len(n.nodeID)-6:])
			}

			// Then sync the entire CRDT state
			if err := n.syncCRDTState(); err != nil {
				log.Printf("❌ CRDT Sync error: %v", err)
			} else {
				counter++
				// Show current CRDT content every sync
				n.printCurrentCRDTContent()
			}
		}
	}
}

// startSubscriber starts the subscriber goroutine
func (n *Node) startSubscriber() {
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
				// Only log real errors, not timeouts
				log.Printf("❌ CRDT Subscription error: %v", err)
				time.Sleep(time.Second)
			}
		}
	}
}

// startHeartbeat starts the heartbeat goroutine
func (n *Node) startHeartbeat() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.heartbeatTicker.C:
			log.Printf("💓 CRDT Subscriber heartbeat - listening for messages...")
		}
	}
}

// syncCRDTState publishes the current CRDT state
func (n *Node) syncCRDTState() error {
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
	msg := Message{
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

// receiveCRDTMessage receives and processes CRDT messages
func (n *Node) receiveCRDTMessage() error {
	// Create a context with timeout to avoid blocking indefinitely
	ctx, cancel := context.WithTimeout(n.ctx, 1*time.Second)
	defer cancel()

	msg, err := n.sub.Next(ctx)
	if err != nil {
		if ctx.Err() != nil {
			// This is a normal timeout, not an error
			return nil
		}
		return fmt.Errorf("failed to receive CRDT message: %w", err)
	}

	// Parse CRDT message
	var crdtMsg Message
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

	// Handle sync request: if we can publish, reply with full state
	if crdtMsg.Type == "sync_request" {
		if n.config.Mode == ModePublish || n.config.Mode == ModeBoth {
			if err := n.syncCRDTState(); err != nil {
				log.Printf("⚠️  Failed to respond to sync request: %v", err)
			} else {
				log.Printf("📤 Responded to sync request from %s", crdtMsg.NodeID[:8])
			}
		}
	}

	log.Printf("📨 Received CRDT message from %s: %s", crdtMsg.NodeID[:8], crdtMsg.Type)
	return nil
}

// applyCRDTSync applies received CRDT state to local engine
func (n *Node) applyCRDTSync(msg Message) error {
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

// sendSyncRequest asks peers to publish their full CRDT state immediately
func (n *Node) sendSyncRequest() error {
	msg := Message{
		Type:      "sync_request",
		NodeID:    n.nodeID,
		Key:       "request",
		Timestamp: time.Now(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal sync request: %w", err)
	}
	return n.topic.Publish(n.ctx, data)
}

// printCurrentCRDTContent shows current CRDT content in a compact format
func (n *Node) printCurrentCRDTContent() {
	allCRDTs := n.engine.GetAllCRDTs()

	fmt.Printf("📋 Current CRDT Content:\n")
	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			fmt.Printf("  📦 %s: %d elements\n", key, len(elements))
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			fmt.Printf("  🔢 %s: %d\n", key, value)
		}
	}
	fmt.Println()
}
