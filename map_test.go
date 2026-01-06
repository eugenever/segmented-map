package segmentedmap

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetGet(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")

	value, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	_, ok = m.Get(2)
	assert.False(t, ok)

	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")

	value, ok = m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	_, ok = m.Get(2)
	assert.False(t, ok)
}

func TestDelete(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")

	value, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	m.Delete(1)
	_, ok = m.Get(1)
	assert.False(t, ok)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")

	value, ok = m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	m.Delete(1)
	_, ok = m.Get(1)
	assert.False(t, ok)
}

func TestDeleteConditional(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")

	value, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	m.DeleteConditional(1, func(v string) bool {
		if v == "value1" {
			return true
		} else {
			return false
		}
	})
	_, ok = m.Get(1)
	assert.False(t, ok)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")

	value, ok = m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, "value1", value)

	m.DeleteConditional(1, func(v string) bool {
		if v == "value1" {
			return true
		} else {
			return false
		}
	})
	_, ok = m.Get(1)
	assert.False(t, ok)
}

func TestLen(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")

	len := m.Len()
	assert.Equal(t, 1, len)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")

	len = m.Len()
	assert.Equal(t, 1, len)
}

func TestKeys(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")
	m.Set(2, "value2")

	keys := m.Keys()
	assert.Equal(t, []int{1, 2}, keys)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")
	m.Set(2, "value2")

	keys = m.Keys()
	assert.Equal(t, []int{2, 1}, keys)
}

func TestValues(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")
	m.Set(2, "value2")

	values := m.Values()
	assert.Equal(t, []string{"value1", "value2"}, values)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")
	m.Set(2, "value2")

	values = m.Values()
	assert.Equal(t, []string{"value2", "value1"}, values)
}

func TestGetOrSet(t *testing.T) {
	m := NewMap[int, string]()
	{
		v, created := m.GetOrSet(0, func() string { return "abc" })
		assert.True(t, created)
		assert.Equal(t, v, "abc")
	}
	{
		v, created := m.GetOrSet(0, func() string { return "xyz" })
		assert.False(t, created)
		assert.Equal(t, v, "abc")
	}

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	{
		v, created := m.GetOrSet(0, func() string { return "abc" })
		assert.True(t, created)
		assert.Equal(t, v, "abc")
	}
	{
		v, created := m.GetOrSet(0, func() string { return "xyz" })
		assert.False(t, created)
		assert.Equal(t, v, "abc")
	}
}

func TestGetAndDelete(t *testing.T) {
	m := NewMap[int, string]()
	m.Set(1, "value1")
	value, exist := m.GetAndDelete(1)
	assert.True(t, exist)
	assert.Equal(t, "value1", value)

	value, exist = m.GetAndDelete(1)
	assert.False(t, exist)
	assert.Equal(t, "", value)

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	m.Set(1, "value1")
	value, exist = m.GetAndDelete(1)
	assert.True(t, exist)
	assert.Equal(t, "value1", value)

	value, exist = m.GetAndDelete(1)
	assert.False(t, exist)
	assert.Equal(t, "", value)
}

func TestUpdate(t *testing.T) {
	m := NewMap[int, string]()
	{
		v, created := m.Update(0, "abc", func(s string) string { return s + "abc" })
		assert.True(t, created)
		assert.Equal(t, v, "abcabc")
	}
	{
		v, created := m.Update(0, "xyz", func(s string) string { return s + "abc" })
		assert.False(t, created)
		assert.Equal(t, v, "abcabcabc")
	}

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	{
		v, created := m.Update(0, "abc", func(s string) string { return s + "abc" })
		assert.True(t, created)
		assert.Equal(t, v, "abcabc")
	}
	{
		v, created := m.Update(0, "xyz", func(s string) string { return s + "abc" })
		assert.False(t, created)
		assert.Equal(t, v, "abcabcabc")
	}

}

func TestUpdateExisting(t *testing.T) {
	m := NewMap[int, string]()
	{
		v, existing := m.UpdateExisting(0, func(s string) string { return s + "abc" })
		assert.False(t, existing)
		assert.Equal(t, zero[string](), v)
	}
	{
		m.Set(0, "abc")
		v, existing := m.UpdateExisting(0, func(s string) string { return s + "abc" })
		assert.True(t, existing)
		assert.Equal(t, v, "abcabc")
	}

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	{
		v, existing := m.UpdateExisting(0, func(s string) string { return s + "abc" })
		assert.False(t, existing)
		assert.Equal(t, zero[string](), v)
	}
	{
		m.Set(0, "abc")
		v, existing := m.UpdateExisting(0, func(s string) string { return s + "abc" })
		assert.True(t, existing)
		assert.Equal(t, v, "abcabc")
	}
}

func TestCalculate(t *testing.T) {
	m := NewMap[int, string]()
	{
		m.Set(0, "abc")
		v, existing := m.Calculate(0, func(s string) any { return s + "abc" })
		assert.True(t, existing)
		assert.Equal(t, v, "abcabc")
	}

	// segmented
	m = NewSegmentedMap[int, string](2, 1)
	{
		m.Set(0, "abc")
		v, existing := m.Calculate(0, func(s string) any { return s + "abc" })
		assert.True(t, existing)
		assert.Equal(t, v, "abcabc")
	}
}

func TestConcurrencyAccess(t *testing.T) {
	m := NewMap[int, string]()
	var wg sync.WaitGroup

	numberGoroutine := 100000

	for i := range numberGoroutine {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, "v"+strconv.Itoa(i))
		}(i)
	}
	wg.Wait()

	for i := range numberGoroutine {
		value, ok := m.Get(i)
		assert.True(t, ok)
		assert.Equal(t, "v"+strconv.Itoa(i), value)
	}

	m = NewSegmentedMap[int, string](256, 1)

	for i := range numberGoroutine {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m.Set(i, "v"+strconv.Itoa(i))
		}(i)
	}
	wg.Wait()

	for i := range numberGoroutine {
		value, ok := m.Get(i)
		assert.True(t, ok)
		assert.Equal(t, "v"+strconv.Itoa(i), value)
	}
}

func BenchmarkSetGet(b *testing.B) {
	m := NewMap[int, string]()
	numberGoroutine := 1

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := range numberGoroutine {
				m.Set(i, "v"+strconv.Itoa(i))
				_, _ = m.Get(i)
			}
		}
	})
}

func BenchmarkSetGetSegmented(b *testing.B) {
	m := NewSegmentedMap[int, string](128, 1)
	numberGoroutine := 1

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := range numberGoroutine {
				m.Set(i, "v"+strconv.Itoa(i))
				_, _ = m.Get(i)
			}
		}
	})
}
