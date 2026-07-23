package genesis

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"clicksync/internal/publication"
)

const (
	officialByronURL   = "https://book.world.dev.cardano.org/environments/mainnet/byron-genesis.json"
	officialShelleyURL = "https://book.world.dev.cardano.org/environments/mainnet/shelley-genesis.json"
)

// TestOfficialMainnetRefreshMatchesEmbedded is the only network refresh path.
// It is opt-in evidence tooling and is not linked into the shipped binary.
func TestOfficialMainnetRefreshMatchesEmbedded(t *testing.T) {
	if os.Getenv("CLICKSYNC_GENESIS_REFRESH_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_GENESIS_REFRESH_INTEGRATION=1 to verify upstream genesis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 30 * time.Second}
	byronJSON := fetchOfficialGenesis(t, ctx, client, officialByronURL, maxByronBytes)
	shelleyJSON := fetchOfficialGenesis(t, ctx, client, officialShelleyURL, maxShelleyBytes)
	refreshed, err := ParseMainnet(byronJSON, shelleyJSON)
	if err != nil {
		t.Fatal(err)
	}
	embedded, err := Mainnet()
	if err != nil {
		t.Fatal(err)
	}
	refreshedDigest, err := publication.FactsDigest(refreshed.Block, refreshed.Source)
	if err != nil {
		t.Fatal(err)
	}
	embeddedDigest, err := publication.FactsDigest(embedded.Block, embedded.Source)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Proof != embedded.Proof ||
		refreshedDigest != embeddedDigest {
		t.Fatal("upstream official genesis differs from the build-embedded bundle")
	}
}

func fetchOfficialGenesis(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	url string,
	maximum int64,
) []byte {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(fmt.Errorf("fetch %s: HTTP %s", url, response.Status))
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maximum+1))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(body)) > maximum {
		t.Fatalf("official genesis %s exceeds %d bytes", url, maximum)
	}
	return body
}
