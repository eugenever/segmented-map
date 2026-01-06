package inthash

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"reflect"
	"unsafe"
)

func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func BytesToString(b []byte) string {
	return unsafe.String(&b[0], len(b))
}

type Hashable interface {
	string | ~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type Hash[T Hashable | any] struct {
	segments int
	mask     uint16
}

func New(segments int) *Hash[any] {
	return &Hash[any]{segments: segments, mask: mask(segments)}
}

func NewHash[T Hashable](segments int) *Hash[T] {
	return &Hash[T]{segments: segments, mask: mask(segments)}
}

func (s *Hash[T]) Segments() int {
	return s.segments
}

func (s *Hash[T]) Get(k T) int {
	var b int
	test := reflect.TypeOf(k).Kind()
	h := fnv.New32()
	switch test {
	case reflect.String:
		h.Write(StringToBytes(reflect.ValueOf(k).String()))
		b = int(h.Sum32())
	case reflect.Int:
		i := uint32(reflect.ValueOf(k).Int())
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Int8:
		i := uint16(reflect.ValueOf(k).Int())
		bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Int16:
		i := uint16(reflect.ValueOf(k).Int())
		bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Int32:
		i := uint32(reflect.ValueOf(k).Int())
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Int64:
		i := uint64(reflect.ValueOf(k).Int())
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Uint:
		i := uint32(reflect.ValueOf(k).Int())
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Uint8:
		i := uint16(reflect.ValueOf(k).Int())
		bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Uint16:
		i := uint16(reflect.ValueOf(k).Int())
		bytes := make([]byte, 2)
		binary.LittleEndian.PutUint16(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Uint32:
		i := uint32(reflect.ValueOf(k).Int())
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Uint64:
		i := uint64(reflect.ValueOf(k).Uint())
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Float32:
		f := uint32(math.Float32bits(float32(reflect.ValueOf(k).Float())))
		e := uint32(float32(reflect.ValueOf(k).Float()))
		i := f + e
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	case reflect.Float64:
		f := uint32(math.Float32bits(float32(reflect.ValueOf(k).Float())))
		e := uint32(float32(reflect.ValueOf(k).Float()))
		i := f + e
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, i)
		h.Write(bytes)
		b = int(h.Sum32())
	default:
		panic("non implemented type")
	}
	hash := b & int(s.mask)
	return hash
}

func mask(segments int) uint16 {
	size := max(segments, 1)
	bitCount := uint16(math.Log2(float64(size)))
	return uint16(1<<bitCount - 1)
}
