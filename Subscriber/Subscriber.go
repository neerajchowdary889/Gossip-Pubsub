package Subscriber

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// SubscriberMetrics holds metrics for the subscriber
type SubscriberMetrics struct {
	MessagesReceived int64
	ReceiveErrors    int64
	ValidationErrors int64 // ✅ Added validation error tracking
	lastReceiveTime  int64 // ✅ Changed to atomic int64 (Unix nano)
	uniquePeersMu    sync.RWMutex
	uniquePeers      map[string]int64 // ✅ Changed to int64 for atomic-safe timestamps
}

// Subscriber represents a message subscriber
type Subscriber struct {
	subscription *pubsub.Subscription
	metrics      *SubscriberMetrics
}

// NewSubscriber creates a new Subscriber instance
func NewSubscriber(subscription *pubsub.Subscription) *Subscriber {
	return &Subscriber{
		subscription: subscription,
		metrics: &SubscriberMetrics{
			uniquePeers: make(map[string]int64),
		},
	}
}

// StartSubscriber starts receiving messages from the subscription
// This is the main entry point called from main.go
func StartSubscriber(ctx context.Context, sub *pubsub.Subscription) {
	subscriber := NewSubscriber(sub)
	subscriber.run(ctx)
}

// run executes the subscriber loop
func (s *Subscriber) run(ctx context.Context) {
	log.Printf("📨 Subscriber started, listening for messages...")

	for {
		select {
		case <-ctx.Done():
			log.Printf("📨 Subscriber stopping...")
			return
		default:
			if err := s.receiveMessage(ctx); err != nil {
				if ctx.Err() != nil {
					// Context cancelled, exit gracefully
					return
				}
				log.Printf("❌ Subscription error: %v", err)
				atomic.AddInt64(&s.metrics.ReceiveErrors, 1)

				// Add exponential backoff for errors
				time.Sleep(time.Second)
			}
		}
	}
}

// receiveMessage receives and processes a single message
func (s *Subscriber) receiveMessage(ctx context.Context) error {
	msg, err := s.subscription.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	// Validate BEFORE processing
	if err := s.validateMessage(msg); err != nil {
		atomic.AddInt64(&s.metrics.ValidationErrors, 1)
		log.Printf("⚠️ Validation failed: %v", err)
		return nil // ✅ Don't treat as fatal error, continue processing
	}

	// Update metrics (thread-safe)
	atomic.AddInt64(&s.metrics.MessagesReceived, 1)
	atomic.StoreInt64(&s.metrics.lastReceiveTime, time.Now().UnixNano())

	// Track unique peers (protected by mutex)
	peerID := msg.ReceivedFrom.String()
	s.metrics.uniquePeersMu.Lock()
	s.metrics.uniquePeers[peerID] = time.Now().UnixNano()
	s.metrics.uniquePeersMu.Unlock()

	// Process the message
	s.processMessage(msg)
	return nil
}

// processMessage processes a received message
func (s *Subscriber) processMessage(msg *pubsub.Message) {
	messageData := string(msg.Data)
	peerID := msg.ReceivedFrom.String()

	log.Printf("📨 Received from %s: %s", peerID, messageData)

	// Additional message processing can be added here
	// For example: validation, filtering, routing, etc.
}

// validateMessage validates the received message
func (s *Subscriber) validateMessage(msg *pubsub.Message) error {
	// Basic validation
	if msg == nil {
		return fmt.Errorf("received nil message")
	}

	if len(msg.Data) == 0 {
		return fmt.Errorf("received empty message")
	}

	if len(msg.Data) > 1024*1024 { // 1MB limit
		return fmt.Errorf("message too large: %d bytes", len(msg.Data))
	}

	// ✅ Validate peer ID
	if msg.ReceivedFrom == "" {
		return fmt.Errorf("message has no sender peer ID")
	}

	return nil
}

// GetMetrics returns current subscriber metrics (thread-safe)
func (s *Subscriber) GetMetrics() SubscriberMetrics {
	s.metrics.uniquePeersMu.RLock()
	// Create a copy of unique peers map
	uniquePeers := make(map[string]int64, len(s.metrics.uniquePeers))
	for peer, timestamp := range s.metrics.uniquePeers {
		uniquePeers[peer] = timestamp
	}
	s.metrics.uniquePeersMu.RUnlock()

	lastRecvNano := atomic.LoadInt64(&s.metrics.lastReceiveTime)

	return SubscriberMetrics{
		MessagesReceived: atomic.LoadInt64(&s.metrics.MessagesReceived),
		ReceiveErrors:    atomic.LoadInt64(&s.metrics.ReceiveErrors),
		ValidationErrors: atomic.LoadInt64(&s.metrics.ValidationErrors),
		lastReceiveTime:  lastRecvNano,
		uniquePeers:      uniquePeers,
	}
}

// GetLastReceiveTime returns the last message receive time (thread-safe)
func (s *Subscriber) GetLastReceiveTime() time.Time {
	nano := atomic.LoadInt64(&s.metrics.lastReceiveTime)
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

// GetUniquePeerCount returns the number of unique peers seen (thread-safe)
func (s *Subscriber) GetUniquePeerCount() int {
	s.metrics.uniquePeersMu.RLock()
	defer s.metrics.uniquePeersMu.RUnlock()
	return len(s.metrics.uniquePeers)
}

// GetRecentPeers returns peers that have sent messages recently (thread-safe)
func (s *Subscriber) GetRecentPeers(since time.Duration) []string {
	var recentPeers []string
	cutoff := time.Now().Add(-since).UnixNano()

	s.metrics.uniquePeersMu.RLock()
	defer s.metrics.uniquePeersMu.RUnlock()

	for peer, timestamp := range s.metrics.uniquePeers {
		if timestamp > cutoff {
			recentPeers = append(recentPeers, peer)
		}
	}

	return recentPeers
}

// ProcessMessageWithHandler processes messages using a custom handler function
func (s *Subscriber) ProcessMessageWithHandler(ctx context.Context, handler func(*pubsub.Message) error) error {
	msg, err := s.subscription.Next(ctx)
	if err != nil {
		return fmt.Errorf("failed to receive message: %w", err)
	}

	// Validate BEFORE custom handler
	if err := s.validateMessage(msg); err != nil {
		atomic.AddInt64(&s.metrics.ValidationErrors, 1)
		log.Printf("⚠️ Validation failed in custom handler: %v", err)
		return nil // Don't treat as fatal
	}

	// Update metrics (thread-safe)
	atomic.AddInt64(&s.metrics.MessagesReceived, 1)
	atomic.StoreInt64(&s.metrics.lastReceiveTime, time.Now().UnixNano())

	// Track unique peers
	peerID := msg.ReceivedFrom.String()
	s.metrics.uniquePeersMu.Lock()
	s.metrics.uniquePeers[peerID] = time.Now().UnixNano()
	s.metrics.uniquePeersMu.Unlock()

	// Call custom handler
	if err := handler(msg); err != nil {
		return fmt.Errorf("message handler error: %w", err)
	}

	return nil
}