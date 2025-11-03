package CRDTSync

import (
	"fmt"
	"strings"
	"time"

	CRDT "GossipPubsub/CRDT"
)

// PrintStats prints detailed synchronization statistics
func (n *Node) PrintStats() {
	peers := n.host.Network().Peers()

	fmt.Println("\n" + strings.Repeat("─", 80))
	fmt.Println("📊 CRDT PERFORMANCE STATS")
	fmt.Println(strings.Repeat("─", 80))
	fmt.Printf("Connected Peers:    %d\n", len(peers))
	fmt.Printf("Mode:              %s\n", string(n.config.Mode))
	fmt.Printf("Sync Interval:     %v\n", n.config.SyncInterval)

	// Get CRDT state
	allCRDTs := n.engine.GetAllCRDTs()
	fmt.Printf("CRDT Objects:       %d\n", len(allCRDTs))
	fmt.Printf("Messages Received: %d\n", n.msgStore.Count())

	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			fmt.Printf("  Set '%s':         %d elements\n", key, len(elements))
			// Show first few elements
			if len(elements) > 0 {
				maxShow := 3
				if len(elements) < maxShow {
					maxShow = len(elements)
				}
				for i := 0; i < maxShow; i++ {
					fmt.Printf("    [%d] %s\n", i+1, elements[i])
				}
				if len(elements) > 3 {
					fmt.Printf("    ... and %d more\n", len(elements)-3)
				}
			}
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			fmt.Printf("  Counter '%s':     %d\n", key, value)
		}
	}

	fmt.Println(strings.Repeat("─", 80) + "\n")
}

// PrintFinalState prints the final CRDT state
func (n *Node) PrintFinalState() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL CRDT STATE")
	fmt.Println(strings.Repeat("=", 80))

	// Print CRDT objects
	allCRDTs := n.engine.GetAllCRDTs()
	for key, crdt := range allCRDTs {
		switch crdt.(type) {
		case *CRDT.LWWSet:
			elements, _ := n.engine.GetSet(key)
			fmt.Printf("\nSet '%s': (%d elements)\n", key, len(elements))
			for i, element := range elements {
				fmt.Printf("  [%d] %s\n", i+1, element)
			}
		case *CRDT.Counter:
			value, _ := n.engine.GetCounter(key)
			fmt.Printf("\nCounter '%s': %d\n", key, value)
		}
	}

	// Print message store
	n.PrintMessageStore()
	fmt.Println(strings.Repeat("=", 80) + "\n")
}

// PrintMessageStore prints the message store contents
func (n *Node) PrintMessageStore() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Printf("CRDT MESSAGE STORE - Total: %d messages\n", n.msgStore.Count())
	fmt.Println(strings.Repeat("=", 80))

	if n.msgStore.Count() == 0 {
		fmt.Println("No CRDT messages received yet.")
		return
	}

	for key, msg := range n.msgStore.GetAll() {
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

// StartStatsPrinter starts a goroutine that prints stats periodically
func (n *Node) StartStatsPrinter(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-n.ctx.Done():
				return
			case <-ticker.C:
				n.PrintStats()
			}
		}
	}()
}
