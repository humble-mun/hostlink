package agent

import (
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
)

func TestProjectNetworks(t *testing.T) {
	tests := []struct {
		name     string
		settings *container.NetworkSettings
		want     map[string]string
	}{
		{name: "nil settings"},
		{name: "empty networks", settings: &container.NetworkSettings{}},
		{
			name: "multiple networks skip empty addresses",
			settings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{
				"bridge": {IPAddress: "172.30.1.5"},
				"empty":  {IPAddress: ""},
				"gpu":    {IPAddress: "10.0.0.8"},
				"nil":    nil,
			}},
			want: map[string]string{"bridge": "172.30.1.5", "gpu": "10.0.0.8"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// When
			got := projectNetworks(test.settings)

			// Then
			if test.want == nil {
				if got != nil {
					t.Fatalf("projected networks = %#v, want nil", got)
				}
				return
			}
			if len(got) != len(test.want) {
				t.Fatalf("projected network count = %d, want %d", len(got), len(test.want))
			}
			for name, wantIP := range test.want {
				if gotIP := got[name]; gotIP != wantIP {
					t.Fatalf("network %q IP = %q, want %q", name, gotIP, wantIP)
				}
			}
		})
	}
}
