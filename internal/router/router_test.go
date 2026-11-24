package router

import "testing"

func TestParseWorkerAddr(t *testing.T) {
	tests := []struct {
		spec  string
		model string
		addr  string
	}{
		{spec: "localhost:9101", model: "workhorse", addr: "localhost:9101"},
		{spec: "frontier=api.example.com:443", model: "frontier", addr: "api.example.com:443"},
		{spec: "workhorse=127.0.0.1:9200", model: "workhorse", addr: "127.0.0.1:9200"},
	}
	for _, tt := range tests {
		model, addr := parseWorkerAddr(tt.spec)
		if model != tt.model || addr != tt.addr {
			t.Fatalf("parseWorkerAddr(%q) = %q, %q; want %q, %q", tt.spec, model, addr, tt.model, tt.addr)
		}
	}
}
