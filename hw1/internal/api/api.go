package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Item struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Quantity  int       `json:"quantity"`
	UpdatedAt time.Time `json:"updated_at"`
}

type itemInput struct {
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
}

type Store struct {
	mu       sync.RWMutex
	nextID   uint64
	items    map[string]Item
	nameToID map[string]string
}

func NewStore() *Store {
	return &Store{items: make(map[string]Item), nameToID: make(map[string]string)}
}

var (
	errItemNotFound = errors.New("item not found")
	errNameConflict = errors.New("an item with this name already exists")
)

func (s *Store) Create(input itemInput) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.nameToID[input.Name]; exists {
		return Item{}, errNameConflict
	}

	s.nextID++
	now := time.Now().UTC()
	item := Item{
		ID:        strconv.FormatUint(s.nextID, 10),
		Name:      input.Name,
		Quantity:  input.Quantity,
		UpdatedAt: now,
	}

	s.items[item.ID] = item
	s.nameToID[item.Name] = item.ID

	return item, nil
}

func (s *Store) Get(id string) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, ok := s.items[id]

	return item, ok
}

func (s *Store) Update(id string, input itemInput) (Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous, ok := s.items[id]
	if !ok {
		return Item{}, errItemNotFound
	}

	if existingID, exists := s.nameToID[input.Name]; exists && existingID != id {
		return Item{}, errNameConflict
	}

	item := Item{
		ID:        id,
		Name:      input.Name,
		Quantity:  input.Quantity,
		UpdatedAt: time.Now().UTC(),
	}
	s.items[id] = item
	if previous.Name != item.Name {
		delete(s.nameToID, previous.Name)
		s.nameToID[item.Name] = item.ID
	}

	return item, nil
}

type API struct {
	store *Store
}

func New(store *Store) *API {
	return &API{store: store}
}

func (a *API) CreateItem(w http.ResponseWriter, r *http.Request) {
	input, status, err := decodeInput(w, r)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}

	item, err := a.store.Create(input)
	if errors.Is(err, errNameConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	w.Header().Set("Location", "/items/"+item.ID)
	writeJSON(w, http.StatusCreated, item)
}

func (a *API) GetItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validItemID(id) {
		writeError(w, http.StatusBadRequest, "item id must be a positive integer")
		return
	}

	item, ok := a.store.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "item not found")
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func (a *API) UpdateItem(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validItemID(id) {
		writeError(w, http.StatusBadRequest, "item id must be a positive integer")
		return
	}

	input, status, err := decodeInput(w, r)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}

	item, err := a.store.Update(id, input)
	if errors.Is(err, errItemNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	if errors.Is(err, errNameConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, item)
}

func decodeInput(w http.ResponseWriter, r *http.Request) (itemInput, int, error) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var input itemInput
	if err := decoder.Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return itemInput{}, http.StatusRequestEntityTooLarge, errors.New("request body is too large")
		}

		return itemInput{}, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return itemInput{}, http.StatusBadRequest, errors.New("request body must contain one JSON object")
	}

	input.Name = strings.TrimSpace(input.Name)
	if input.Name == "" {
		return itemInput{}, http.StatusUnprocessableEntity, errors.New("name is required")
	}

	if input.Quantity < 0 {
		return itemInput{}, http.StatusUnprocessableEntity, errors.New("quantity must be non-negative")
	}

	return input, 0, nil
}

func validItemID(id string) bool {
	value, err := strconv.ParseUint(id, 10, 64)

	return err == nil && value > 0
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
