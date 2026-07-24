package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var errContainerNotFound = errors.New("container not found")

type dockerAPI interface {
	DiskUsage(context.Context) (diskUsage, error)
	IngestionContainers(context.Context) ([]containerSummary, error)
	Inspect(context.Context, string) (container, error)
	Stop(context.Context, string, time.Duration) (bool, error)
}

type dockerClient struct {
	http    *http.Client
	baseURL string
}

type diskUsage struct {
	Volumes    []volumeUsage    `json:"Volumes"`
	Containers []containerUsage `json:"Containers"`
}

type volumeUsage struct {
	Name      string            `json:"Name"`
	Labels    map[string]string `json:"Labels"`
	UsageData *struct {
		Size int64 `json:"Size"`
	} `json:"UsageData"`
}

type containerUsage struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
	SizeRW int64             `json:"SizeRw"`
}

type containerSummary struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
	State  string            `json:"State"`
	Status string            `json:"Status"`
}

type container struct {
	ID     string            `json:"Id"`
	Name   string            `json:"Name"`
	Labels map[string]string `json:"-"`
	Config struct {
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State containerState `json:"State"`
}

type containerState struct {
	Status     string           `json:"Status"`
	Running    bool             `json:"Running"`
	Paused     bool             `json:"Paused"`
	Restarting bool             `json:"Restarting"`
	OOMKilled  bool             `json:"OOMKilled"`
	Dead       bool             `json:"Dead"`
	ExitCode   int              `json:"ExitCode"`
	Error      string           `json:"Error"`
	StartedAt  string           `json:"StartedAt"`
	FinishedAt string           `json:"FinishedAt"`
	Health     *containerHealth `json:"Health"`
}

type containerHealth struct {
	Status        string `json:"Status"`
	FailingStreak int    `json:"FailingStreak"`
}

func newDockerClient(socketPath string) *dockerClient {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		},
		ForceAttemptHTTP2: false,
	}
	return &dockerClient{
		http: &http.Client{
			Transport: transport,
			Timeout:   75 * time.Second,
		},
		baseURL: "http://docker",
	}
}

func newDockerHTTPClient(endpoint string) *dockerClient {
	return &dockerClient{
		http: &http.Client{
			Timeout: 75 * time.Second,
		},
		baseURL: strings.TrimRight(endpoint, "/"),
	}
}

func (client *dockerClient) DiskUsage(ctx context.Context) (diskUsage, error) {
	var usage diskUsage
	const path = "/system/df?type=volume&type=container"
	if err := client.getJSON(ctx, path, &usage); err != nil {
		return diskUsage{}, fmt.Errorf("read Docker disk usage: %w", err)
	}
	return usage, nil
}

func (client *dockerClient) IngestionContainers(
	ctx context.Context,
) ([]containerSummary, error) {
	filters, err := json.Marshal(map[string][]string{
		"label": {
			ownershipLabelKey + "=" + ownershipLabelValue,
			kindLabelKey + "=" + ingestionKind,
		},
	})
	if err != nil {
		return nil, err
	}
	var values []containerSummary
	path := "/containers/json?all=1&filters=" + url.QueryEscape(string(filters))
	if err := client.getJSON(ctx, path, &values); err != nil {
		return nil, fmt.Errorf("list Clicksync ingestion containers: %w", err)
	}
	return values, nil
}

func (client *dockerClient) Inspect(
	ctx context.Context,
	idOrName string,
) (container, error) {
	var value container
	path := "/containers/" + url.PathEscape(idOrName) + "/json"
	err := client.getJSON(ctx, path, &value)
	if errors.Is(err, errContainerNotFound) {
		return container{}, errContainerNotFound
	}
	if err != nil {
		return container{}, fmt.Errorf("inspect container: %w", err)
	}
	value.Labels = value.Config.Labels
	return value, nil
}

func (client *dockerClient) Stop(
	ctx context.Context,
	id string,
	timeout time.Duration,
) (alreadyStopped bool, err error) {
	seconds := int64(timeout / time.Second)
	path := "/containers/" + url.PathEscape(id) + "/stop?t=" +
		strconv.FormatInt(seconds, 10)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		client.baseURL+path,
		nil,
	)
	if err != nil {
		return false, err
	}
	response, err := client.http.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusNoContent:
		return false, nil
	case http.StatusNotModified:
		return true, nil
	default:
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return false, fmt.Errorf(
			"Docker stop returned %s: %s",
			response.Status,
			strings.TrimSpace(string(message)),
		)
	}
}

func (client *dockerClient) getJSON(
	ctx context.Context,
	path string,
	target any,
) error {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		client.baseURL+path,
		nil,
	)
	if err != nil {
		return err
	}
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return errContainerNotFound
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf(
			"Docker API returned %s: %s",
			response.Status,
			strings.TrimSpace(string(message)),
		)
	}
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}
