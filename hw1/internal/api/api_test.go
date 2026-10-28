package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestItemLifecycle(t *testing.T) {
	application := New(NewStore())
	mux := http.NewServeMux()
	mux.HandleFunc("POST /items", application.CreateItem)
	mux.HandleFunc("GET /items/{id}", application.GetItem)
	mux.HandleFunc("PUT /items/{id}", application.UpdateItem)

	created := requestItem(t, mux, http.MethodPost, "/items", `{"name":"book","quantity":2}`, http.StatusCreated)
	if created.ID != "1" || created.Name != "book" || created.Quantity != 2 {
		t.Fatalf("unexpected created item: %+v", created)
	}

	got := requestItem(t, mux, http.MethodGet, "/items/1", "", http.StatusOK)
	if got != created {
		t.Fatalf("GET item = %+v, want %+v", got, created)
	}

	updated := requestItem(t, mux, http.MethodPut, "/items/1", `{"name":"notebook","quantity":5}`, http.StatusOK)
	if updated.ID != "1" || updated.Name != "notebook" || updated.Quantity != 5 {
		t.Fatalf("unexpected updated item: %+v", updated)
	}
}

func TestValidationAndNotFound(t *testing.T) {
	application := New(NewStore())
	mux := http.NewServeMux()
	mux.HandleFunc("POST /items", application.CreateItem)
	mux.HandleFunc("GET /items/{id}", application.GetItem)

	requestItem(t, mux, http.MethodPost, "/items", `{`, http.StatusBadRequest)
	requestItem(t, mux, http.MethodPost, "/items", `{"name":"","quantity":-1}`, http.StatusUnprocessableEntity)
	requestItem(t, mux, http.MethodPost, "/items", `{"name":"book","quantity":1}`, http.StatusCreated)
	requestItem(t, mux, http.MethodPost, "/items", `{"name":"book","quantity":2}`, http.StatusConflict)
	requestItem(t, mux, http.MethodGet, "/items/not-a-number", "", http.StatusBadRequest)
	requestItem(t, mux, http.MethodGet, "/items/404", "", http.StatusNotFound)
}

func requestItem(t *testing.T, handler http.Handler, method, path, body string, wantStatus int) Item {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body=%s", method, path, response.Code, wantStatus, response.Body.String())
	}

	if wantStatus >= http.StatusBadRequest {
		return Item{}
	}

	var item Item
	if err := json.NewDecoder(response.Body).Decode(&item); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	return item
}
