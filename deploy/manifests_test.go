package deploy

import (
	"io/fs"
	"os"
	"regexp"
	"strings"
	"testing"
)

// BSV Broadcaster workload manifests must use the env names Viper actually
// binds (store.aerospike.* → BSV_BROADCASTER_STORE_AEROSPIKE_*) and an image
// tag the build workflow publishes (ghcr.io/bsv-blockchain/broadcaster with a
// vX.Y.Z release tag or a 40-character commit SHA), so a plain
// `kubectl apply -f deploy/` pulls. A placeholder tag, the mutable :latest
// tag, and a foreign GHCR namespace are rejected. Issues #92 and #93.
var (
	// workloadImageTag captures the tag of each broadcaster image line.
	workloadImageTag = regexp.MustCompile(`(?m)^\s*image:\s*ghcr\.io/bsv-blockchain/broadcaster:(\S+)\s*$`)
	// publishedImageTag matches the immutable tags .github/workflows/build.yml
	// pushes: a vX.Y.Z release tag or the full commit SHA.
	publishedImageTag = regexp.MustCompile(`^(v\d+\.\d+\.\d+|[0-9a-f]{40})$`)
)

func TestBroadcasterWorkloadManifests(t *testing.T) {
	// DirFS + fs.ReadFile keeps the path inside this package directory.
	// os.ReadFile on a glob result trips gosec G304 even though the names
	// are not attacker-controlled.
	root := os.DirFS(".")
	files, err := fs.Glob(root, "*.yaml")
	if err != nil {
		t.Fatalf("glob deploy yaml: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no deploy yaml files found; test must run from the deploy directory")
	}

	var workloads int
	for _, name := range files {
		body, err := fs.ReadFile(root, name)
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
		tags := workloadImageTag.FindAllStringSubmatch(text, -1)
		if len(tags) == 0 {
			t.Errorf("%s image is not ghcr.io/bsv-blockchain/broadcaster:<tag>", name)
		}
		for _, m := range tags {
			if !publishedImageTag.MatchString(m[1]) {
				t.Errorf("%s image tag %q is not a vX.Y.Z release tag or a 40-character commit SHA; the build workflow publishes no such immutable tag", name, m[1])
			}
		}
	}
	if workloads < 8 {
		t.Fatalf("found %d broadcaster workload manifests, want at least 8", workloads)
	}
}
