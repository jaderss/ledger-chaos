package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Toxiproxy struct {
	base string
	http *http.Client
}

func NewToxiproxy(base string) *Toxiproxy {
	return &Toxiproxy{base: base, http: &http.Client{}}
}

func (t *Toxiproxy) do(ctx context.Context, method, path string, body any, ok ...int) error {
	var reader *bytes.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.base+path, reader)
	if err != nil {
		return err
	}
	res, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	for _, code := range ok {
		if res.StatusCode == code {
			return nil
		}
	}
	return fmt.Errorf("%s %s: status %d", method, path, res.StatusCode)
}

func (t *Toxiproxy) SetEnabled(ctx context.Context, proxy string, enabled bool) error {
	return t.do(ctx, http.MethodPost, "/proxies/"+proxy, map[string]bool{"enabled": enabled}, http.StatusOK)
}

func (t *Toxiproxy) AddToxic(ctx context.Context, proxy, name, kind string, attributes map[string]int) error {
	toxic := map[string]any{"name": name, "type": kind, "stream": "downstream", "attributes": attributes}
	return t.do(ctx, http.MethodPost, "/proxies/"+proxy+"/toxics", toxic, http.StatusOK)
}

func (t *Toxiproxy) RemoveToxic(ctx context.Context, proxy, name string) error {
	return t.do(ctx, http.MethodDelete, "/proxies/"+proxy+"/toxics/"+name, nil, http.StatusNoContent, http.StatusNotFound)
}
