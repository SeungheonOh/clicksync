package config

import "testing"

func TestValidateBoundsAndIndependentPeers(t *testing.T) {
	cfg := Config{
		ClickHouseHost:     "clickhouse",
		ClickHousePort:     9000,
		ClickHouseUser:     "clicksync",
		ClickHousePassword: "secret",
		ClickHouseDatabase: "clicksync",
		NetworkMagic:       MainnetMagic,
		Peers:              append([]string(nil), DefaultPeers...),
		Corroboration:      2,
		Start:              "intersection",
		QueueCapacity:      4,
		WarningBytes:       DefaultWarningLimit,
		ActiveLimitBytes:   DefaultActiveLimit,
		ProjectLimitBytes:  DefaultProjectLimit,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.Peers[1] = cfg.Peers[0]
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted duplicate corroboration peers")
	}
}
