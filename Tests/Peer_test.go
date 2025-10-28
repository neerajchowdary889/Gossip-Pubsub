package Tests

import (
	"testing"
	"GossipPubsub/Peer"
)

func TestLoadPeerID(t *testing.T) {
	peerID, err := Peer.LoadPeerID()
	if err != nil {
		t.Fatalf("Failed to load peer ID: %v", err)
	}
	t.Logf("Peer ID: %s", peerID)
}