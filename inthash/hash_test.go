package inthash

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func IntInRange(min, max int) int {
	return min + rand.Intn(max+1-min)
}

func FloatInRange(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

func TestStringHash(t *testing.T) {
	hash := NewHash[string](0)
	assert.Equal(t, 0, hash.Get("test-1"))
	assert.Equal(t, 0, hash.Get("test-2"))

	hash = NewHash[string](2)
	assert.Equal(t, 1, hash.Get("test-1"))
	assert.Equal(t, 0, hash.Get("test-2"))
	assert.Equal(t, 1, hash.Get("test-3"))

	hash = NewHash[string](4)
	assert.Equal(t, 3, hash.Get("test-1"))
	assert.Equal(t, 0, hash.Get("test-2"))
	assert.Equal(t, 1, hash.Get("test-3"))
	assert.Equal(t, 2, hash.Get("test-4"))

	hash = NewHash[string](8)
	assert.Equal(t, 3, hash.Get("test-1"))
	assert.Equal(t, 0, hash.Get("test-2"))
	assert.Equal(t, 1, hash.Get("test-3"))
	assert.Equal(t, 6, hash.Get("test-4"))
	assert.Equal(t, 7, hash.Get("test-5"))
	assert.Equal(t, 4, hash.Get("test-6"))
}

func TestIntHash(t *testing.T) {
	hash := NewHash[int](0)
	assert.Equal(t, 0, hash.Get(1))
	assert.Equal(t, 0, hash.Get(2))

	hash = NewHash[int](2)
	assert.Equal(t, 0, hash.Get(1))
	assert.Equal(t, 1, hash.Get(2))

	hash = NewHash[int](4)
	assert.Equal(t, 2, hash.Get(1))
	assert.Equal(t, 3, hash.Get(2))
	assert.Equal(t, 0, hash.Get(3))
	assert.Equal(t, 1, hash.Get(4))

	hash = NewHash[int](8)
	assert.Equal(t, 2, hash.Get(1))
	assert.Equal(t, 7, hash.Get(2))
	assert.Equal(t, 4, hash.Get(3))
	assert.Equal(t, 1, hash.Get(4))
	assert.Equal(t, 6, hash.Get(5))
	assert.Equal(t, 3, hash.Get(6))
	assert.Equal(t, 0, hash.Get(7))
	assert.Equal(t, 5, hash.Get(8))

	assert.Equal(t, 7, hash.Get(65459879))
	assert.Equal(t, 4, hash.Get(32132))

	hash = NewHash[int](8)
	assert.Equal(t, 4, hash.Get(670985037))
	assert.Equal(t, 1, hash.Get(670984781))
	assert.Equal(t, 5, hash.Get(670722637))
}

func TestFloatHash(t *testing.T) {
	hash := NewHash[float64](0)
	assert.Equal(t, 0, hash.Get(1.1))
	assert.Equal(t, 0, hash.Get(1.2))

	hash = NewHash[float64](4)
	assert.Equal(t, 0, hash.Get(1.1))
	assert.Equal(t, 1, hash.Get(1.2))
	assert.Equal(t, 3, hash.Get(1.3))
	assert.Equal(t, 0, hash.Get(1.4))

	hash = NewHash[float64](8)
	assert.Equal(t, 4, hash.Get(1.1))
	assert.Equal(t, 5, hash.Get(1.2))
	assert.Equal(t, 3, hash.Get(1.3))
	assert.Equal(t, 0, hash.Get(1.4))
	assert.Equal(t, 5, hash.Get(1.5))
	assert.Equal(t, 4, hash.Get(1.6))
	assert.Equal(t, 5, hash.Get(1.7))
	assert.Equal(t, 3, hash.Get(1.8))

	assert.Equal(t, 3, hash.Get(32132.0))
	assert.Equal(t, 7, hash.Get(32132.564))
}

func TestAnyHash(t *testing.T) {
	hash := New(0)
	assert.Equal(t, 0, hash.Get("test-1"))
	assert.Equal(t, 0, hash.Get("test-2"))

	hash = New(2)
	assert.Equal(t, 0, hash.Get(1))
	assert.Equal(t, 1, hash.Get(2))

	hash = New(4)
	assert.Equal(t, 0, hash.Get(1.1))
	assert.Equal(t, 1, hash.Get(1.2))
	assert.Equal(t, 3, hash.Get(1.3))
	assert.Equal(t, 0, hash.Get(1.4))

	type custom int
	hash = New(8)
	assert.Equal(t, 2, hash.Get(custom(1)))
	assert.Equal(t, 7, hash.Get(custom(2)))
	assert.Equal(t, 4, hash.Get(custom(3)))
	assert.Equal(t, 1, hash.Get(custom(4)))
	assert.Equal(t, 6, hash.Get(custom(5)))
	assert.Equal(t, 3, hash.Get(custom(6)))
	assert.Equal(t, 0, hash.Get(custom(7)))
	assert.Equal(t, 5, hash.Get(custom(8)))
}

func TestHashStability(t *testing.T) {
	hash := NewHash[int](10)
	intMap := make(map[int]int)
	size := 10

	for i := range size {
		intMap[i] = hash.Get(i)
	}
	for range size {
		i := IntInRange(0, size)
		h := hash.Get(i)
		assert.Equal(t, intMap[i], h)
	}
}
