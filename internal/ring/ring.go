// Package ring implements a consistent-hash ring. Adding or removing a worker
// remaps only its own keys, which keeps prefix affinity stable during scale
// events and failures.
package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// Ring maps keys to worker IDs using virtual nodes.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	keys     []uint32
	table    map[uint32]string
}

func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = 128
	}
	return &Ring{replicas: replicas, table: map[uint32]string{}}
}

// Set replaces the entire membership in one rebuild.
func (r *Ring) Set(ids []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = nil
	r.table = make(map[uint32]string, len(ids)*r.replicas)
	for _, id := range ids {
		for i := 0; i < r.replicas; i++ {
			h := hash32(id + "#" + strconv.Itoa(i))
			r.keys = append(r.keys, h)
			r.table[h] = id
		}
	}
	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i] < r.keys[j] })
}

func (r *Ring) Add(id string) {
	r.mu.RLock()
	current := make([]string, 0, len(r.table))
	seen := map[string]bool{}
	for _, v := range r.table {
		if !seen[v] {
			seen[v] = true
			current = append(current, v)
		}
	}
	r.mu.RUnlock()
	for _, existing := range current {
		if existing == id {
			return
		}
	}
	current = append(current, id)
	r.Set(current)
}

func (r *Ring) Remove(id string) {
	r.mu.RLock()
	next := make([]string, 0, len(r.table))
	seen := map[string]bool{}
	for _, v := range r.table {
		if v == id || seen[v] {
			continue
		}
		seen[v] = true
		next = append(next, v)
	}
	r.mu.RUnlock()
	r.Set(next)
}

// Lookup returns the owner of key, or "" when the ring is empty.
func (r *Ring) Lookup(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.keys) == 0 {
		return ""
	}
	h := hash32(key)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= h })
	if i == len(r.keys) {
		i = 0
	}
	return r.table[r.keys[i]]
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
