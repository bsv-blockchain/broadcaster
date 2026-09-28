package deploy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// BSV Broadcaster workload manifests must use the env names Viper actually
// binds (store.aerospike.* → BSV_BROADCASTER_STORE_AEROSPIKE_*) and the image
// the build workflow publishes (ghcr.io/bsv-blockchain/broadcaster:<git sha>),
// not a mutable :latest tag or a foreign GHCR namespace. Issues #92 and #93.
func TestBroadcasterWorkloadManifests(t *testing.T) {
	files, err := filepath.Glob("*.yaml")
	if err != nil {
		t.Fatalf("glob deploy yaml: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no deploy yaml files found; test must run from the deploy directory")
	}

	var workloads int
	for _, name := range files {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		text := string(body)
		if strings.Contains(text, "BSV_BROADCASTER_AEROSPIKE_") {
			t.Errorf("%s sets BSV_BROADCASTER_AEROSPIKE_*; Viper binds store.aerospike.* as BSV_BROADCASTER_STORE_AEROSPIKE_*", name)
		}
		if strings.Contains(text, "ghcr.io/galt-tr/") {
			t.Errorf("%s references ghcr.io/galt-tr; CI publishes ghcr.io/bsv-blockchain/broadcaster", name)
		}
		if !strings.Contains(text, "--mode") {
			continue
		}
		workloads++
		if !strings.Contains(text, "name: BSV_BROADCASTER_STORE_AEROSPIKE_HOSTS") ||
			!strings.Contains(text, "name: BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE") {
			t.Errorf("%s is missing BSV_BROADCASTER_STORE_AEROSPIKE_HOSTS or BSV_BROADCASTER_STORE_AEROSPIKE_NAMESPACE", name)
		}
		if !strings.Contains(text, "image: ghcr.io/bsv-blockchain/broadcaster:") {
			t.Errorf("%s image is not ghcr.io/bsv-blockchain/broadcaster:<tag>", name)
		}
		if strings.Contains(text, "image: ghcr.io/bsv-blockchain/broadcaster:latest") {
			t.Errorf("%s pins the mutable :latest tag; use the git SHA published by the build workflow", name)
		}
	}
	if workloads < 8 {
		t.Fatalf("found %d broadcaster workload manifests, want at least 8", workloads)
	}
}
