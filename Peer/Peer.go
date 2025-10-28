package Peer

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerConfig struct {
	PeerID string `json:"peer_id"`
	PrivKey []byte `json:"private_key"`
}

// LoadPeerID loads the PeerID from Peer/Peer.json
// Function to load the PeerID from the Peer/Peer.json
// - if the Peer.json is not available then generate a new file for the PeerID with name Peer/Peer.json
func LoadPeerID() (string, error) {
	data, err := os.ReadFile("Peer/Peer.json")
	if err != nil {
		// Fallback to create a new PeerID file
		peerID, err := CreatePeerID()
		if err != nil {
			return "", fmt.Errorf("failed to create peer ID: %w", err)
		}
		return peerID, nil
	}
	
	var config PeerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		// Fallback to create a new PeerID file
		return "", fmt.Errorf("failed to unmarshal peer ID: %w", err)
	}
	
	return config.PeerID, nil
}

// CreatePeerID generates a new libp2p PeerID and saves it to Peer/Peer.json
func CreatePeerID() (string, error) {
	// Generate a new Ed25519 key pair
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		return "", err
	}
	
	// Get the PeerID from the public key
	peerID, err := peer.IDFromPublicKey(priv.GetPublic())
	if err != nil {
		return "", err
	}
	
	// Marshal the private key
	privBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return "", err
	}
	
	// Create config
	config := PeerConfig{
		PeerID:  peerID.String(),
		PrivKey: privBytes,
	}
	
	// Save to file
	data, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	
	if err := os.WriteFile("Peer/Peer.json", data, 0644); err != nil {
		return "", err
	}
	
	return peerID.String(), nil
}

// LoadPrivateKey loads the private key from Peer/Peer.json
func LoadPrivateKey() (crypto.PrivKey, error) {
	data, err := os.ReadFile("Peer/Peer.json")
	if err != nil {
		return nil, err
	}
	
	var config PeerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	
	return crypto.UnmarshalPrivateKey(config.PrivKey)
}