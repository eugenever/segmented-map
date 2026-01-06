package segmentedmap

import (
	hash "segmentedmap/inthash"
	"sync"
)

// Map is a thread-safe map divided into segments, provides high concurrent performance
type Map[K hash.Hashable, V any] struct {
	segment   []map[K]V       // Map segments
	mutex     []*sync.RWMutex // Mutexes for synchronizing access
	hash      *hash.Hash[K]   // Hash generation struct
	segmented bool            // If map has more than 1 segment
}

// NewMap creates a new thread-safe map
func NewMap[K hash.Hashable, V any]() *Map[K, V] {
	return NewSegmentedMap[K, V](1, 1)
}

// NewSegmentedMap creates a new thread-safe map divided into segments
func NewSegmentedMap[K hash.Hashable, V any](segmentCount int, segmentCapacity int) *Map[K, V] {
	segmented := false
	hash := hash.NewHash[K](segmentCount)
	if hash.Segments() > 1 {
		segmented = true
	}
	segments := make([]map[K]V, segmentCount)
	mutex := make([]*sync.RWMutex, segmentCount)
	for i := range segments {
		segments[i] = make(map[K]V, segmentCapacity)
		mutex[i] = &sync.RWMutex{}
	}
	return &Map[K, V]{
		segment:   segments,
		hash:      hash,
		mutex:     mutex,
		segmented: segmented,
	}
}

// Set inserts or updates the value for the given key into map.
func (s *Map[K, V]) Set(k K, v V) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)
	s.segment[h][k] = v
}

// Get retrieves the value associated with the given key.
// It returns the value and a boolean indicating whether the key exists.
func (s *Map[K, V]) Get(k K) (V, bool) {
	h := s.hash.Get(k)
	s.rLock(h)
	defer s.rUnlock(h)
	if v, ok := s.segment[h][k]; ok {
		return v, ok
	} else {
		return zero[V](), false
	}
}

// Delete removes the key-value.
func (s *Map[K, V]) Delete(keys ...K) {
	hashes := make(map[int][]K, max(len(keys), s.hash.Segments()))
	for _, k := range keys {
		hashes[s.hash.Get(k)] = append(hashes[s.hash.Get(k)], k)
	}
	for h, hashKeys := range hashes {
		s.lock(h)
		for _, k := range hashKeys {
			delete(s.segment[h], k)
		}
		s.unlock(h)
	}
}

// DeleteConditional retrieves the value associated with the key and computes condition value.
// It returns the value removed or zero and a boolean indicating whether a key is exist.
func (s *Map[K, V]) DeleteConditional(k K, calculate func(v V) bool) (v V, existing bool) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		condition := calculate(v)
		if condition {
			delete(s.segment[h], k)
		}
		return v, false
	} else {
		return zero[V](), false
	}
}

// Len returns the number of key-value pairs in the map.
func (s *Map[K, V]) Len() int {
	_len := 0
	for i := range s.segment {
		s.rLock(i)
		_len = _len + len(s.segment[i])
		s.rUnlock(i)
	}
	return _len
}

// Keys return all keys from safe map
func (s *Map[K, V]) Keys() []K {
	keys := make([]K, 0, len(s.segment[0]))
	for i := range s.segment {
		s.rLock(i)
		for k := range s.segment[i] {
			keys = append(keys, k)
		}
		s.rUnlock(i)
	}
	return keys
}

// Keys return all values from safe map
func (s *Map[K, V]) Values() []V {
	values := make([]V, 0, len(s.segment[0]))
	for i := range s.segment {
		s.rLock(i)
		for _, v := range s.segment[i] {
			values = append(values, v)
		}
		s.rUnlock(i)
	}
	return values
}

// GetOrSet retrieves the value associated with the key, or computes and stores a new value if the key does not exist.
// It returns the value and a boolean indicating whether a new value was created.
func (s *Map[K, V]) GetOrSet(k K, calculate func() V) (v V, created bool) {
	if v, ok := s.Get(k); ok {
		return v, false
	}

	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		return v, false
	}

	v = calculate()
	s.segment[h][k] = v

	return v, true
}

// GetAndDelete retrieves the value associated with the key if it exists and delete the key from the map.
// It returns the value and a boolean indicating whether the key is exists.
func (s *Map[K, V]) GetAndDelete(k K) (v V, exist bool) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		delete(s.segment[h], k)
		return v, true
	}

	return zero[V](), false
}

// Update retrieves the value associated with the key and computes new value based on the previous one.
// If value does not exist it uses the provided one to perform the calculation. Return the new values and a boolean indicating whether a new value was created.
func (s *Map[K, V]) Update(k K, defaultValue V, calculate func(V) V) (v V, created bool) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		value := calculate(v)
		s.segment[h][k] = value
		return value, false
	} else {
		value := calculate(defaultValue)
		s.segment[h][k] = value
		return value, true
	}
}

// Update retrieves the value associated with the key and computes new values based on the previous one.
// If value does not exist it do nothing. Return the new values and a boolean indicating whether the key is exists.
func (s *Map[K, V]) UpdateExisting(k K, calculate func(V) V) (v V, exist bool) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		value := calculate(v)
		s.segment[h][k] = value
		return value, true
	}

	return zero[V](), false
}

// Calculate retrieves the value associated with the key and computes some value.
// If value does not exist it returns nil. Returns the calculated valuer and a boolean indicating whether the key is exists.
func (s *Map[K, V]) Calculate(k K, calculate func(V) any) (r any, exist bool) {
	h := s.hash.Get(k)
	s.lock(h)
	defer s.unlock(h)

	if v, ok := s.segment[h][k]; ok {
		value := calculate(v)
		return value, true
	}

	return nil, false
}

// Range iterates over all key-value pairs in the map, applying the given function.
// If the function returns false, the iteration stops.
func (s *Map[K, V]) Range(run func(k K, v V) bool) {
	for i := range s.segment {
		s.rLock(i)
		for k, v := range s.segment[i] {
			if ok := run(k, v); !ok { // Stop iteration if the callback returns false
				s.rUnlock(i)
				return
			}
		}
		s.rUnlock(i)
	}
}

func (s *Map[K, V]) rLock(i int) {
	s.mutex[i].RLock()
}

func (s *Map[K, V]) rUnlock(i int) {
	s.mutex[i].RUnlock()
}

func (s *Map[K, V]) lock(i int) {
	s.mutex[i].Lock()
}

func (s *Map[K, V]) unlock(i int) {
	s.mutex[i].Unlock()
}

func zero[T any]() (x T) {
	return x
}
