package crdt

import (
	"errors"

	"strings"
)

// validateInput validates common input parameters for set operations
func validateInput(nodeID, key, element string) error {
	if strings.TrimSpace(nodeID) == "" {
		return errors.New("nodeID cannot be empty")
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("key cannot be empty")
	}
	if strings.TrimSpace(element) == "" {
		return errors.New("element cannot be empty")
	}

	// Check for reasonable length limits
	if len(nodeID) > 256 {
		return errors.New("nodeID too long (max 256 characters)")
	}
	if len(key) > 256 {
		return errors.New("key too long (max 256 characters)")
	}
	if len(element) > 1024 {
		return errors.New("element too long (max 1024 characters)")
	}

	return nil
}

// validateCounterInput validates input parameters for counter operations
func validateCounterInput(nodeID, key string, delta uint64) error {
	if strings.TrimSpace(nodeID) == "" {
		return errors.New("nodeID cannot be empty")
	}
	if strings.TrimSpace(key) == "" {
		return errors.New("key cannot be empty")
	}
	if delta == 0 {
		return errors.New("delta cannot be zero")
	}
	if delta > 1000000 { // Reasonable upper limit
		return errors.New("delta too large (max 1,000,000)")
	}

	// Check for reasonable length limits
	if len(nodeID) > 256 {
		return errors.New("nodeID too long (max 256 characters)")
	}
	if len(key) > 256 {
		return errors.New("key too long (max 256 characters)")
	}

	return nil
}
