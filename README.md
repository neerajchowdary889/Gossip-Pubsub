# Gossip PubSub

A Go implementation of a decentralized publish-subscribe system using libp2p and GossipSub protocol.

## Features

- **Decentralized**: No central server required, peer-to-peer communication
- **Configurable**: Environment variable based configuration
- **Observable**: Built-in metrics and logging
- **Resilient**: Proper error handling and graceful shutdown
- **Extensible**: Clean architecture with separate publisher/subscriber modules

## Architecture

```
main.go
├── GossipApp (main application)
├── Publisher/ (message publishing)
└── Subscriber/ (message receiving)
```

## Quick Start

1. **Install dependencies**:
```bash
go mod tidy
```

2. **Run the application**:
```bash
go run main.go
```

3. **Run multiple instances** to see peer-to-peer communication:
```bash
# Terminal 1
go run main.go

# Terminal 2 (different port)
PORT=8081 go run main.go
```

## Configuration

The application can be configured using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TOPIC_NAME` | `gossip-demo` | Name of the pubsub topic |
| `PORT` | `0` | Port to listen on (0 = random) |
| `ENABLE_MDNS` | `true` | Enable mDNS peer discovery |
| `PUBLISH_INTERVAL` | `1s` | Interval between published messages |
| `HEARTBEAT_INTERVAL` | `400ms` | GossipSub heartbeat interval |
| `GOSSIP_SUB_D` | `12` | GossipSub degree |
| `GOSSIP_SUB_DLO` | `8` | GossipSub degree low |
| `GOSSIP_SUB_DHI` | `16` | GossipSub degree high |
| `HISTORY_LENGTH` | `12` | Message history length |
| `HISTORY_GOSSIP` | `6` | History gossip count |
| `GOSSIP_FACTOR` | `0.7` | Gossip factor |
| `GOSSIP_RETRANSMISSION` | `4` | Retransmission count |

## Usage Examples

### Basic Usage
```bash
# Default configuration
go run main.go

# Custom topic and port
TOPIC_NAME=my-topic PORT=8080 go run main.go
```

### Multiple Peers
```bash
# Peer 1
go run main.go

# Peer 2 (different port)
PORT=8081 go run main.go

# Peer 3 (different port)
PORT=8082 go run main.go
```

### Custom Publishing Interval
```bash
# Publish every 5 seconds
PUBLISH_INTERVAL=5s go run main.go

# Publish every 100ms
PUBLISH_INTERVAL=100ms go run main.go
```

## API Reference

### Publisher Package

#### `StartPublisher(ctx, topic, interval)`
Starts the publisher goroutine that publishes messages at regular intervals.

#### `NewPublisher(topic, interval)`
Creates a new Publisher instance.

#### `PublishCustomMessage(ctx, message)`
Publishes a custom message.

#### `PublishBatch(ctx, messages)`
Publishes multiple messages in a batch.

#### `GetMetrics()`
Returns current publisher metrics.

### Subscriber Package

#### `StartSubscriber(ctx, subscription)`
Starts the subscriber goroutine that receives and processes messages.

#### `NewSubscriber(subscription)`
Creates a new Subscriber instance.

#### `ProcessMessageWithHandler(ctx, handler)`
Processes messages using a custom handler function.

#### `GetMetrics()`
Returns current subscriber metrics.

#### `GetUniquePeerCount()`
Returns the number of unique peers seen.

#### `GetRecentPeers(since)`
Returns peers that have sent messages recently.

## Metrics

The application provides built-in metrics:

### Publisher Metrics
- `MessagesPublished`: Total messages published
- `PublishErrors`: Total publish errors
- `LastPublishTime`: Timestamp of last publish

### Subscriber Metrics
- `MessagesReceived`: Total messages received
- `ReceiveErrors`: Total receive errors
- `LastReceiveTime`: Timestamp of last receive
- `UniquePeers`: Map of unique peers and their last activity

## Graceful Shutdown

The application handles graceful shutdown on SIGINT/SIGTERM signals:
- Stops publishing/subscribing
- Closes topics and subscriptions
- Closes libp2p host
- Waits for all goroutines to finish

## Development

### Project Structure
```
├── main.go              # Main application entry point
├── Publisher/
│   └── Publisher.go     # Publisher implementation
├── Subsciber/
│   └── Subscriber.go    # Subscriber implementation
├── go.mod               # Go module definition
└── README.md            # This file
```

### Adding Features

1. **New Message Types**: Extend the message processing in `Subscriber.go`
2. **Custom Handlers**: Use `ProcessMessageWithHandler` for custom message processing
3. **Additional Metrics**: Add new metrics to the `Metrics` structs
4. **Configuration**: Add new environment variables to the `Config` struct

### Testing

```bash
# Run tests
go test ./...

# Run with coverage
go test -cover ./...

# Run benchmarks
go test -bench=. ./...
```

## Dependencies

- `github.com/libp2p/go-libp2p` - libp2p networking library
- `github.com/libp2p/go-libp2p-pubsub` - GossipSub implementation
- `github.com/libp2p/go-libp2p/p2p/discovery/mdns` - mDNS peer discovery

## License

MIT License
