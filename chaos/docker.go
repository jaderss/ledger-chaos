package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type Docker struct {
	http    *http.Client
	project string
	socket  string
}

func NewDocker(socket, project string) *Docker {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	return &Docker{http: &http.Client{Transport: transport}, project: project, socket: socket}
}

func (d *Docker) container(ctx context.Context, service string) (string, error) {
	filters, _ := json.Marshal(map[string][]string{"label": {
		"com.docker.compose.project=" + d.project,
		"com.docker.compose.service=" + service,
	}})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://docker/containers/json?all=1&filters="+url.QueryEscape(string(filters)), nil)
	if err != nil {
		return "", err
	}
	res, err := d.http.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	var containers []struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(res.Body).Decode(&containers); err != nil {
		return "", err
	}
	if len(containers) == 0 {
		return "", fmt.Errorf("no container for service %s", service)
	}
	return containers[0].ID, nil
}

func (d *Docker) action(ctx context.Context, service, action string, alreadyDone ...string) error {
	id, err := d.container(ctx, service)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/containers/"+id+"/"+action, nil)
	if err != nil {
		return err
	}
	res, err := d.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNoContent || res.StatusCode == http.StatusNotModified {
		return nil
	}
	message, _ := io.ReadAll(res.Body)
	for _, done := range alreadyDone {
		if strings.Contains(string(message), done) {
			return nil
		}
	}
	return fmt.Errorf("%s %s: status %d %s", action, service, res.StatusCode, strings.TrimSpace(string(message)))
}

func (d *Docker) Kill(ctx context.Context, service string) error {
	return d.action(ctx, service, "kill?signal=SIGKILL")
}

func (d *Docker) Start(ctx context.Context, service string) error {
	return d.action(ctx, service, "start")
}

func (d *Docker) Stop(ctx context.Context, service string) error {
	return d.action(ctx, service, "stop?t=0")
}

func (d *Docker) Restart(ctx context.Context, service string) error {
	return d.action(ctx, service, "restart?t=0")
}

func (d *Docker) Pause(ctx context.Context, service string) error {
	return d.action(ctx, service, "pause")
}

func (d *Docker) Unpause(ctx context.Context, service string) error {
	return d.action(ctx, service, "unpause", "is not paused")
}
