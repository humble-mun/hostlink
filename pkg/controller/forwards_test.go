package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/go-logr/logr"
)

type forwardHTTPRequest struct {
	Method string
	Path   string
	Body   string
}

type notFoundPortStore struct{ portStore }

func (notFoundPortStore) release(context.Context, uint32) error {
	return errPortNotFound
}

func newForwardRouter(store portStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	svc := &service{
		logger:    logr.Discard(),
		store:     store,
		rangeFrom: 2000,
		rangeTo:   2002,
	}
	svc.RegisterRoute(router)
	return router
}

func forwardRequest(t *testing.T, router http.Handler, args forwardHTTPRequest) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(args.Method, args.Path, strings.NewReader(args.Body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func createForward(t *testing.T, router http.Handler, mapping portMapping) forwardResponse {
	t.Helper()
	body, err := json.Marshal(mapping)
	if err != nil {
		t.Fatalf("marshal forward request: %v", err)
	}
	recorder := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/" + mapping.AgentID + "/forwards",
		Body:   string(body),
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create forward status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	return decodeForward(t, recorder)
}

func decodeForward(t *testing.T, recorder *httptest.ResponseRecorder) forwardResponse {
	t.Helper()
	var response forwardResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode forward response: %v", err)
	}
	return response
}

func decodeForwards(t *testing.T, recorder *httptest.ResponseRecorder) []forwardResponse {
	t.Helper()
	var response []forwardResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode forwards response: %v", err)
	}
	return response
}

func decodeError(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var response map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return response["error"]
}

func TestCreateForward_returnsAllocatedMapping_whenTargetValid(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/agent-a/forwards",
		Body:   `{"target":"172.30.1.5:8080"}`,
	})
	response := decodeForward(t, recorder)

	// Then
	if response.Port < 2000 || response.Port > 2002 {
		t.Fatalf("allocated port = %d, want within 2000-2002", response.Port)
	}
	if response.AgentID != "agent-a" || response.Target != "172.30.1.5:8080" {
		t.Fatalf("mapping = %#v, want agent and target echoed", response)
	}
	if response.ContainerID != "" {
		t.Fatalf("container ID = %q, want empty", response.ContainerID)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw forward response: %v", err)
	}
	if _, exists := raw["container_id"]; exists {
		t.Fatalf("response = %s, want container_id omitted", recorder.Body.String())
	}
}

func TestCreateForward_usesRequestedPort_whenFreeAndInRange(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/agent-a/forwards",
		Body:   `{"target":"172.30.1.5:8080","port":2001}`,
	})
	response := decodeForward(t, recorder)

	// Then
	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if response.Port != 2001 {
		t.Fatalf("allocated port = %d, want requested port 2001", response.Port)
	}
}

func TestCreateForward_returnsBadRequest_whenRequestedPortOutOfRange(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "below range", body: `{"target":"172.30.1.5:8080","port":1999}`},
		{name: "above range", body: `{"target":"172.30.1.5:8080","port":2003}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			router := newForwardRouter(newPortStore(logr.Discard(), nil))

			// When
			recorder := forwardRequest(t, router, forwardHTTPRequest{
				Method: http.MethodPost,
				Path:   "/api/v1/agents/agent-a/forwards",
				Body:   test.body,
			})

			// Then
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if message := decodeError(t, recorder); message != "port must be within 2000-2002" {
				t.Fatalf("error = %q, want %q", message, "port must be within 2000-2002")
			}
		})
	}
}

func TestCreateForward_returnsConflict_whenRequestedPortInUse(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))
	first := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/agent-a/forwards",
		Body:   `{"target":"172.30.1.5:8080","port":2001}`,
	})
	if first.Code != http.StatusCreated {
		t.Fatalf("first create status = %d, want %d: %s", first.Code, http.StatusCreated, first.Body.String())
	}

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/agent-b/forwards",
		Body:   `{"target":"172.30.1.5:8081","port":2001}`,
	})

	// Then
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
	if message := decodeError(t, recorder); message != "requested port already in use" {
		t.Fatalf("error = %q, want %q", message, "requested port already in use")
	}
}

func TestCreateForward_returnsBadRequest_whenTargetInvalid(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing port", body: `{"target":"172.30.1.5"}`},
		{name: "empty host", body: `{"target":":8080"}`},
		{name: "zero port", body: `{"target":"172.30.1.5:0"}`},
		{name: "out of range port", body: `{"target":"172.30.1.5:65536"}`},
		{name: "malformed JSON", body: `{"target":`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			router := newForwardRouter(newPortStore(logr.Discard(), nil))

			// When
			recorder := forwardRequest(t, router, forwardHTTPRequest{
				Method: http.MethodPost,
				Path:   "/api/v1/agents/agent-a/forwards",
				Body:   test.body,
			})

			// Then
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}

func TestCreateForward_returnsConflict_whenRangeExhausted(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))
	for range 3 {
		createForward(t, router, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})
	}

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{
		Method: http.MethodPost,
		Path:   "/api/v1/agents/agent-a/forwards",
		Body:   `{"target":"172.30.1.5:8080"}`,
	})

	// Then
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
	if message := decodeError(t, recorder); message != "no free port in range" {
		t.Fatalf("error = %q, want %q", message, "no free port in range")
	}
}

func TestForwardEndpoints_returnServiceUnavailable_whenForwardingDisabled(t *testing.T) {
	// Given
	router := newForwardRouter(nil)
	tests := []forwardHTTPRequest{
		{Method: http.MethodPost, Path: "/api/v1/agents/agent-a/forwards", Body: `{"target":"172.30.1.5:8080"}`},
		{Method: http.MethodGet, Path: "/api/v1/agents/agent-a/forwards"},
		{Method: http.MethodGet, Path: "/api/v1/forwards"},
		{Method: http.MethodDelete, Path: "/api/v1/forwards/2000"},
	}
	for _, test := range tests {
		t.Run(test.Method+" "+test.Path, func(t *testing.T) {
			// When
			recorder := forwardRequest(t, router, test)

			// Then
			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
			}
			if message := decodeError(t, recorder); message != "port forwarding is disabled" {
				t.Fatalf("error = %q, want %q", message, "port forwarding is disabled")
			}
		})
	}
}

func TestListAgentForwards_filtersAndSorts_whenMappingsExist(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))
	createForward(t, router, portMapping{AgentID: "other", Target: "172.30.1.5:8080"})
	createForward(t, router, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8081"})
	createForward(t, router, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8082"})

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodGet, Path: "/api/v1/agents/agent-a/forwards"})

	// Then
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	forwards := decodeForwards(t, recorder)
	if len(forwards) != 2 || forwards[0].Port != 2001 || forwards[1].Port != 2002 {
		t.Fatalf("forwards = %#v, want sorted agent-a ports 2001 and 2002", forwards)
	}
	if forwards[0].AgentID != "agent-a" || forwards[1].AgentID != "agent-a" {
		t.Fatalf("forwards = %#v, want only agent-a", forwards)
	}
}

func TestListAgentForwards_returnsEmptyArray_whenNoMappingsExist(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodGet, Path: "/api/v1/agents/agent-a/forwards"})

	// Then
	forwards := decodeForwards(t, recorder)
	if recorder.Code != http.StatusOK || forwards == nil || len(forwards) != 0 {
		t.Fatalf("status = %d, forwards = %#v, want 200 and []", recorder.Code, forwards)
	}
}

func TestListAllForwards_returnsMappingsSortedByPort(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))
	createForward(t, router, portMapping{AgentID: "agent-c", Target: "172.30.1.5:8080"})
	createForward(t, router, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8081"})
	createForward(t, router, portMapping{AgentID: "agent-b", Target: "172.30.1.5:8082"})

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodGet, Path: "/api/v1/forwards"})

	// Then
	forwards := decodeForwards(t, recorder)
	if recorder.Code != http.StatusOK || len(forwards) != 3 {
		t.Fatalf("status = %d, forwards = %#v, want three mappings", recorder.Code, forwards)
	}
	for index, forward := range forwards {
		if forward.Port != uint32(2000+index) {
			t.Fatalf("forward[%d].Port = %d, want %d", index, forward.Port, 2000+index)
		}
	}
}

func TestDeleteForward_removesMapping_whenPortExists(t *testing.T) {
	// Given
	router := newForwardRouter(newPortStore(logr.Discard(), nil))
	created := createForward(t, router, portMapping{AgentID: "agent-a", Target: "172.30.1.5:8080"})

	// When
	recorder := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodDelete, Path: "/api/v1/forwards/2000"})

	// Then
	if recorder.Code != http.StatusNoContent || recorder.Body.Len() != 0 {
		t.Fatalf("status = %d, body = %q, want 204 with no body", recorder.Code, recorder.Body.String())
	}
	list := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodGet, Path: "/api/v1/forwards"})
	forwards := decodeForwards(t, list)
	if len(forwards) != 0 || created.Port != 2000 {
		t.Fatalf("created = %#v, forwards = %#v, want port 2000 removed", created, forwards)
	}
}

func TestDeleteForward_returnsErrors_whenPortMissingOrInvalid(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		want  int
		error string
		store portStore
	}{
		{name: "missing", path: "/api/v1/forwards/2000", want: http.StatusNotFound, error: "forward not found", store: notFoundPortStore{newPortStore(logr.Discard(), nil)}},
		{name: "invalid", path: "/api/v1/forwards/not-a-port", want: http.StatusBadRequest, error: "invalid port", store: newPortStore(logr.Discard(), nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Given
			router := newForwardRouter(test.store)

			// When
			recorder := forwardRequest(t, router, forwardHTTPRequest{Method: http.MethodDelete, Path: test.path})

			// Then
			if recorder.Code != test.want {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, test.want, recorder.Body.String())
			}
			if message := decodeError(t, recorder); message != test.error {
				t.Fatalf("error = %q, want %q", message, test.error)
			}
		})
	}
}
