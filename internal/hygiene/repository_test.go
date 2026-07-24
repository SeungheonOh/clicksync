package hygiene

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
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
	}
	for _, relRoot := range []string{"cmd", "internal", "migrations", "config"} {
		err := filepath.WalkDir(
			filepath.Join(root, relRoot),
			func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				if !entry.Type().IsRegular() ||
					strings.HasSuffix(entry.Name(), "_test.go") {
					return nil
				}
				rel, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				executable = append(executable, rel)
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
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
		"cpus:",
		"mem_limit:",
		"pids_limit:",
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

func TestComposeStopGraceExceedsAggregateShutdownBudget(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	compose, err := os.ReadFile(filepath.Join(root, "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	runtimePath := filepath.Join(root, "internal", "ingest", "runtime.go")
	shutdownBudget := durationConstant(t, runtimePath, "shutdownTimeout")
	match := regexp.MustCompile(
		`(?m)^\s+stop_grace_period:\s*([0-9]+)s\s*$`,
	).FindSubmatch(compose)
	if len(match) != 2 {
		t.Fatal("clicksync service requires one numeric stop_grace_period in seconds")
	}
	graceSeconds, err := strconv.ParseInt(string(match[1]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	grace := time.Duration(graceSeconds) * time.Second
	const minimumRuntimeOverhead = 10 * time.Second
	if grace < shutdownBudget+minimumRuntimeOverhead {
		t.Fatalf(
			"stop grace %s does not cover shared finalization/audit budget %s plus %s overhead",
			grace,
			shutdownBudget,
			minimumRuntimeOverhead,
		)
	}
}

func TestComposePublishesClickHousePortsAndConwayStart(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	compose, err := os.ReadFile(filepath.Join(root, "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	const httpHostMapping = `"0.0.0.0:${CLICKHOUSE_HTTP_PORT:-18125}:8123"`
	if !strings.Contains(string(compose), httpHostMapping) {
		t.Fatalf("compose HTTP host mapping does not contain %s", httpHostMapping)
	}
	const nativeHostMapping = `"0.0.0.0:${CLICKHOUSE_NATIVE_PORT:-19000}:9000"`
	if !strings.Contains(string(compose), nativeHostMapping) {
		t.Fatalf("compose native host mapping does not contain %s", nativeHostMapping)
	}
	if !strings.Contains(
		string(compose),
		`CLICKHOUSE_NATIVE_PORT: "9000"`,
	) {
		t.Fatal("Clicksync internal native port must remain clickhouse:9000")
	}
	example, err := os.ReadFile(filepath.Join(root, ".env.example"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(example), "CLICKHOUSE_NATIVE_PORT=19000") {
		t.Fatal("example must select the collision-free host native port 19000")
	}
	const queueDefault = `CLICKSYNC_QUEUE_CAPACITY: ${CLICKSYNC_QUEUE_CAPACITY:-32}`
	if !strings.Contains(string(compose), queueDefault) {
		t.Fatal("compose must default to one complete 32-block receive range")
	}
	if !strings.Contains(string(example), "CLICKSYNC_QUEUE_CAPACITY=32") {
		t.Fatal("example must select one complete 32-block receive range")
	}
	const conwayPredecessor = "133660799:e757d57eb8dc9500a61c60a39fadb63d9be6973ba96ae337fd24453d4d15c343"
	if !strings.Contains(string(compose), conwayPredecessor) {
		t.Fatal("compose default must start at the Conway predecessor")
	}
	if !strings.Contains(string(example), "CLICKSYNC_START_POINT="+conwayPredecessor) {
		t.Fatal("example must start at the Conway predecessor")
	}
}

func durationConstant(t *testing.T, path string, name string) time.Duration {
	t.Helper()
	source, err := parser.ParseFile(
		token.NewFileSet(),
		path,
		nil,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	var result time.Duration
	ast.Inspect(source, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for index, ident := range spec.Names {
			if ident.Name != name || index >= len(spec.Values) {
				continue
			}
			product, ok := spec.Values[index].(*ast.BinaryExpr)
			if !ok || product.Op != token.MUL {
				return false
			}
			count, ok := product.X.(*ast.BasicLit)
			if !ok || count.Kind != token.INT {
				return false
			}
			unit, ok := product.Y.(*ast.SelectorExpr)
			if !ok || unit.Sel.Name != "Second" {
				return false
			}
			pkg, ok := unit.X.(*ast.Ident)
			if !ok || pkg.Name != "time" {
				return false
			}
			seconds, parseErr := strconv.ParseInt(count.Value, 10, 64)
			if parseErr == nil {
				result = time.Duration(seconds) * time.Second
			}
			return false
		}
		return true
	})
	if result <= 0 {
		t.Fatalf("positive %s = <integer> * time.Second not found in %s", name, path)
	}
	return result
}
