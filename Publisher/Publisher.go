package Publisher

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// PublisherMetrics holds metrics for the publisher
type PublisherMetrics struct {
	MessagesPublished int64
	PublishErrors     int64
	lastPublishTime   int64 // ✅ Changed to atomic int64 (Unix nano)
	BytesPublished    int64 // ✅ Added bytes tracking
}

// Publisher represents a message publisher
type Publisher struct {
	topic    *pubsub.Topic
	metrics  *PublisherMetrics
	interval time.Duration
}

// NewPublisher creates a new Publisher instance
func NewPublisher(topic *pubsub.Topic, interval time.Duration) *Publisher {
	return &Publisher{
		topic:    topic,
		metrics:  &PublisherMetrics{},
		interval: interval,
	}
}

// StartPublisher starts publishing messages at regular intervals
// This is the main entry point called from main.go
func StartPublisher(ctx context.Context, topic *pubsub.Topic, interval time.Duration) {
	publisher := NewPublisher(topic, interval)
	publisher.run(ctx)
}

// run executes the publisher loop
func (p *Publisher) run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	counter := int64(0)
	log.Printf("📤 Publisher started, publishing every %v", p.interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("📤 Publisher stopping...")
			return
		case <-ticker.C:
			if err := p.publishMessage(ctx, counter); err != nil {
				log.Printf("❌ Publish error: %v", err)
				atomic.AddInt64(&p.metrics.PublishErrors, 1)
			} else {
				counter++
			}
		}
	}
}

// publishMessage publishes a single message
func (p *Publisher) publishMessage(ctx context.Context, counter int64) error {
	msg := fmt.Sprintf("message-%d @ %s", counter, time.Now().Format(time.RFC3339Nano))
	msgBytes := []byte(msg)

	if err := p.topic.Publish(ctx, msgBytes); err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// ✅ Update all metrics atomically
	atomic.AddInt64(&p.metrics.MessagesPublished, 1)
	atomic.AddInt64(&p.metrics.BytesPublished, int64(len(msgBytes)))
	atomic.StoreInt64(&p.metrics.lastPublishTime, time.Now().UnixNano())

	log.Printf("📤 Published: %s", msg)
	return nil
}

// GetMetrics returns current publisher metrics (thread-safe)
func (p *Publisher) GetMetrics() PublisherMetrics {
	lastPubNano := atomic.LoadInt64(&p.metrics.lastPublishTime)

	return PublisherMetrics{
		MessagesPublished: atomic.LoadInt64(&p.metrics.MessagesPublished),
		PublishErrors:     atomic.LoadInt64(&p.metrics.PublishErrors),
		lastPublishTime:   lastPubNano,
		BytesPublished:    atomic.LoadInt64(&p.metrics.BytesPublished),
	}
}

// GetLastPublishTime returns the last publish time (thread-safe)
func (p *Publisher) GetLastPublishTime() time.Time {
	nano := atomic.LoadInt64(&p.metrics.lastPublishTime)
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

// PublishCustomMessage publishes a custom message
func (p *Publisher) PublishCustomMessage(ctx context.Context, message string) error {
	msgBytes := []byte(message)

	if err := p.topic.Publish(ctx, msgBytes); err != nil {
		atomic.AddInt64(&p.metrics.PublishErrors, 1)
		return fmt.Errorf("failed to publish custom message: %w", err)
	}

	// ✅ Update all metrics atomically
	atomic.AddInt64(&p.metrics.MessagesPublished, 1)
	atomic.AddInt64(&p.metrics.BytesPublished, int64(len(msgBytes)))
	atomic.StoreInt64(&p.metrics.lastPublishTime, time.Now().UnixNano())

	log.Printf("📤 Published custom message: %s", message)
	return nil
}

// PublishBatch publishes multiple messages in a batch
// ✅ Returns detailed results including success count
func (p *Publisher) PublishBatch(ctx context.Context, messages []string) error {
	var errors []error
	successCount := int64(0)
	totalBytes := int64(0)

	for i, msg := range messages {
		msgBytes := []byte(msg)

		if err := p.topic.Publish(ctx, msgBytes); err != nil {
			errors = append(errors, fmt.Errorf("message[%d] '%s': %w", i, msg, err))
			atomic.AddInt64(&p.metrics.PublishErrors, 1)
		} else {
			successCount++
			totalBytes += int64(len(msgBytes))
		}
	}

	// ✅ Update metrics for successful publishes only
	if successCount > 0 {
		atomic.AddInt64(&p.metrics.MessagesPublished, successCount)
		atomic.AddInt64(&p.metrics.BytesPublished, totalBytes)
		atomic.StoreInt64(&p.metrics.lastPublishTime, time.Now().UnixNano())
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch publish: %d/%d succeeded, %d failed: %v", 
			successCount, len(messages), len(errors), errors)
	}

	log.Printf("📤 Published batch of %d messages (%d bytes)", len(messages), totalBytes)
	return nil
}

// PublishWithRetry publishes a message with retry logic
// ✅ New function for reliability testing
func (p *Publisher) PublishWithRetry(ctx context.Context, message string, maxRetries int) error {
	msgBytes := []byte(message)
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := p.topic.Publish(ctx, msgBytes); err != nil {
			lastErr = err
			log.Printf("⚠️ Publish attempt %d/%d failed: %v", attempt+1, maxRetries+1, err)
			
			if attempt < maxRetries {
				// Exponential backoff
				backoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(backoff):
					continue
				}
			}
			continue
		}

		// Success
		atomic.AddInt64(&p.metrics.MessagesPublished, 1)
		atomic.AddInt64(&p.metrics.BytesPublished, int64(len(msgBytes)))
		atomic.StoreInt64(&p.metrics.lastPublishTime, time.Now().UnixNano())

		if attempt > 0 {
			log.Printf("✅ Published after %d retries: %s", attempt, message)
		}
		return nil
	}

	atomic.AddInt64(&p.metrics.PublishErrors, 1)
	return fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// GetThroughput calculates messages per second (thread-safe)
func (p *Publisher) GetThroughput(duration time.Duration) float64 {
	messages := atomic.LoadInt64(&p.metrics.MessagesPublished)
	seconds := duration.Seconds()
	if seconds == 0 {
		return 0
	}
	return float64(messages) / seconds
}