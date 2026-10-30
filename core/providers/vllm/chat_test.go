package vllm

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schemas "github.com/maximhq/bifrost/core/schemas"
)

func TestChatCompletion_PreservesExplicitOutputCeilingOnWire(t *testing.T) {
	for _, limit := range []int{1, 8, 15, 16} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/stream=%t", limit, streaming), func(t *testing.T) {
				captured := make(chan int, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body struct {
						MaxCompletionTokens int `json:"max_completion_tokens"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						http.Error(w, "bad JSON", 400)
						return
					}
					captured <- body.MaxCompletionTokens
					if streaming {
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "data: {\"id\":\"test\",\"object\":\"chat.completion.chunk\",\"model\":\"gemma\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						fmt.Fprint(w, `{"id":"test","object":"chat.completion","model":"gemma","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"length"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
					}
				}))
				defer server.Close()
				provider := newTestVLLMResponsesStreamProvider(t, false, false)
				ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
				request := &schemas.BifrostChatRequest{Provider: schemas.VLLM, Model: "gemma",
					Input: []schemas.ChatMessage{{Role: schemas.ChatMessageRoleUser,
						Content: &schemas.ChatMessageContent{ContentStr: schemas.Ptr("Count to fifty.")}}},
					Params: &schemas.ChatParameters{MaxCompletionTokens: &limit}}
				if streaming {
					stream, err := provider.ChatCompletionStream(ctx, noopPostHookRunner, nil, testVLLMKey(server.URL), request)
					if err != nil {
						t.Fatal(err)
					}
					for _, chunk := range drainStreamWithTimeout(t, stream, 5*time.Second) {
						if chunk != nil && chunk.BifrostError != nil {
							t.Fatal(chunk.BifrostError)
						}
					}
				} else if _, err := provider.ChatCompletion(ctx, testVLLMKey(server.URL), request); err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-captured:
					if got != limit {
						t.Fatalf("caller limit %d became %d on the upstream wire", limit, got)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("upstream received no request")
				}
			})
		}
	}
}

// TestChatCompletion_ExtraParamsForwardedAutomatically verifies that provider-specific
// extra params (e.g. chat_template_kwargs) are forwarded to vLLM without requiring
// the caller to set BifrostContextKeyPassthroughExtraParams on the context.
func TestChatCompletion_ExtraParamsForwardedAutomatically(t *testing.T) {
	t.Parallel()

	var capturedBody map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(body, &capturedBody); err != nil {
			http.Error(w, "json error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"id": "chatcmpl-test",
			"object": "chat.completion",
			"created": 1234567890,
			"model": "gemma",
			"choices": [{
				"index": 0,
				"message": {"role": "assistant", "content": "Hello!"},
				"finish_reason": "stop"
			}],
			"usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8}
		}`)
	}))
	defer server.Close()

	provider := newTestVLLMProvider()
	key := schemas.Key{
		ID:    "test-key",
		Value: schemas.SecretVar{Val: "test-api-key"},
		VLLMKeyConfig: &schemas.VLLMKeyConfig{
			URL: schemas.SecretVar{Val: server.URL},
		},
	}

	// Intentionally do NOT set BifrostContextKeyPassthroughExtraParams — the provider
	// should set it automatically.
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)

	hello := "Hello"
	req := &schemas.BifrostChatRequest{
		Provider: schemas.VLLM,
		Model:    "gemma",
		Input: []schemas.ChatMessage{
			{
				Role:    schemas.ChatMessageRoleUser,
				Content: &schemas.ChatMessageContent{ContentStr: &hello},
			},
		},
		Params: &schemas.ChatParameters{
			ExtraParams: map[string]interface{}{
				"chat_template_kwargs": map[string]interface{}{
					"enable_thinking": true,
				},
			},
		},
	}

	_, bifrostErr := provider.ChatCompletion(ctx, key, req)
	if bifrostErr != nil {
		t.Fatalf("ChatCompletion returned error: %v", bifrostErr.Error.Message)
	}

	if capturedBody == nil {
		t.Fatal("mock server did not receive a request body")
	}

	rawKwargs, ok := capturedBody["chat_template_kwargs"]
	if !ok {
		t.Fatalf("chat_template_kwargs missing from outgoing request body; got keys: %v", keys(capturedBody))
	}

	kwargsMap, ok := rawKwargs.(map[string]interface{})
	if !ok {
		t.Fatalf("expected chat_template_kwargs to be an object, got %T", rawKwargs)
	}
	if kwargsMap["enable_thinking"] != true {
		t.Fatalf("expected enable_thinking=true, got %v", kwargsMap["enable_thinking"])
	}
}

func keys(m map[string]interface{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
