package hygiene

import (
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestRepositoryHasOneGoIngestionPath(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	var forbidden []string
	var composeFiles []string
	var envExamples []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch rel {
			case ".git", "clickout", "clickhouse-data", "docs/evidence", ".cache":
				return filepath.SkipDir
			}
			return nil
		}
		base := entry.Name()
		ext := strings.ToLower(filepath.Ext(base))
		if slices.Contains([]string{".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}, ext) {
			forbidden = append(forbidden, rel)
		}
		if slices.Contains([]string{"package.json", "package-lock.json", "npm-shrinkwrap.json", "yarn.lock", "pnpm-lock.yaml", "tsconfig.json"}, base) {
			forbidden = append(forbidden, rel)
		}
		if base == "compose.yaml" || base == "compose.yml" || strings.HasPrefix(base, "docker-compose") {
			composeFiles = append(composeFiles, rel)
		}
		if base == ".env.example" {
			envExamples = append(envExamples, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(forbidden) != 0 {
		t.Fatalf("legacy runtime files remain: %v", forbidden)
	}
	if !slices.Equal(composeFiles, []string{"compose.yaml"}) {
		t.Fatalf("compose files = %v", composeFiles)
	}
	if !slices.Equal(envExamples, []string{".env.example"}) {
		t.Fatalf("env examples = %v", envExamples)
	}
	for _, oldPath := range []string{"p2p", "schema", "src", "test"} {
		entries, err := os.ReadDir(filepath.Join(root, oldPath))
		if err == nil && len(entries) != 0 {
			t.Fatalf("legacy runtime directory %s remains populated", oldPath)
		}
	}
	mainSource, err := os.ReadFile(filepath.Join(root, "cmd", "clicksync", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(mainSource))
	for _, analytical := range []string{`"trace"`, `"utxo"`, `"address"`, `"bfs"`, `"query"`} {
		if strings.Contains(lower, analytical) {
			t.Fatalf("root ingestion command contains analytical surface %s", analytical)
		}
	}
}

func TestExecutableConfigurationHasNoBridgeOrOldGeneration(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	for _, rel := range []string{".env.example", "compose.yaml", "Dockerfile"} {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(data))
		for _, forbidden := range []string{"ogmios", "bridge", "clicksync_v1", "clicksync_v2", "clicksync-v1", "clicksync-v2"} {
			if strings.Contains(lower, forbidden) {
				t.Fatalf("%s contains forbidden legacy marker %q", rel, forbidden)
			}
		}
	}
}

func TestOnlyNativeBinaryNormalizerModelRemains(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	for _, rel := range []string{
		"internal/normalize/bundle.go",
		"internal/normalize/verify.go",
		"internal/model/facts.go",
	} {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{
			"DatumCBORHex",
			"DatumCBOR",
			"AddressHex",
			"AssetNameHex",
			"FeeKnown",
			"json:\"",
			"func BlockFacts(",
		} {
			if strings.Contains(string(data), forbidden) {
				t.Fatalf("%s contains legacy normalizer symbol %q", rel, forbidden)
			}
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "internal", "normalize"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == "normalize.go" || entry.Name() == "types.go" {
			t.Fatalf("legacy normalizer file remains: %s", entry.Name())
		}
	}
}

func TestExecutableRuntimeHasNoArtificialSyncOrStorageCeiling(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	executable := []string{
		".env.example",
		"Dockerfile",
		"compose.yaml",
		"cmd/clicksync/main.go",
		"internal/config/config.go",
		"internal/ingest/runtime.go",
		"internal/syncer/supervisor.go",
	}
	forbidden := []string{
		"max_blocks",
		"stop_at_tip",
		"100gb",
		"100 gb",
		"100gib",
		"storage quota",
		"disk quota",
		"free_space",
		"free-space",
		"disk walker",
		"maxreconnectfailures",
		"max_reconnect",
		"stop after blocks",
		"block limit",
		"tip limit",
		"runtime limit",
	}
	for _, rel := range executable {
		data, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(data))
		for _, marker := range forbidden {
			if strings.Contains(lower, marker) {
				t.Fatalf("%s contains artificial runtime ceiling %q", rel, marker)
			}
		}
	}
	mainSource, err := os.ReadFile(filepath.Join(root, "cmd", "clicksync", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, removedCommand := range []string{`"storage"`, `"lease"`} {
		if strings.Contains(string(mainSource), removedCommand) {
			t.Fatalf("root CLI retains removed command %s", removedCommand)
		}
	}
}
