package roar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func fill(c container, b uint16) {
	for i := range c.data() {
		c.data()[i] = b
	}
}

func TestModify(t *testing.T) {
	data := make([]byte, 32)
	s := toUint64Slice(data)
	for i := 0; i < len(s); i++ {
		s[i] = uint64(i)
	}

	o := toUint64Slice(data)
	for i := 0; i < len(o); i++ {
		require.Equal(t, uint64(i), o[i])
	}
}

func TestContainer(t *testing.T) {
	ra := NewRoaringArray(2)

	offset := ra.newContainer(128)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(128), getSize(ra.data[offset:]))
	require.Equal(t, uint16(0), c.get(indexCardinality))

	fill(c, 0xFF)
	for i := range c.data() {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), c.data()[i], "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), c.data()[i], "at index: %d", i)
		}
	}

	offset2 := ra.newContainer(64) // Add a second container.
	c2 := ra.getContainer(offset2)
	require.Equal(t, uint32(64), getSize(ra.data[offset2:]))
	fill(c2, 0xEE)

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset)
	offset2 += 128

	// Check if the second container is correct.
	c2 = ra.getContainer(offset2)
	require.Equal(t, uint32(64), getSize(ra.data[offset2:]))
	require.Equal(t, 28, len(c2.data()))
	for _, val := range c2.data() {
		require.Equal(t, uint16(0xEE), val)
	}

	// Check if the first container is correct.
	c = ra.getContainer(offset)
	require.Equal(t, uint16(256), getSize(ra.data[offset:]))
	require.Equal(t, 124, len(c.data()))
	for i := range c.data() {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), c.data()[i], "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), c.data()[i], "at index: %d", i)
		}
	}
}

func TestKey(t *testing.T) {
	ra := NewRoaringArray(2)
	for i := 1; i <= 10; i++ {
		ra.Add(uint64(i))
	}

	off, has := ra.keys.getValue(0)
	require.True(t, has)
	c := ra.getContainer(off)
	require.Equal(t, uint16(10), c.get(indexCardinality))

	// Create 10 containers
	for i := 0; i < 10; i++ {
		ra.Add(uint64(i)<<16 + 1)
	}

	for i := 0; i < 10; i++ {
		ra.Add(uint64(i)<<16 + 2)
	}

	for i := 1; i < 10; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(2), c.get(indexCardinality))
	}

	// Do add in the reverse order.
	for i := 19; i >= 10; i-- {
		ra.Add(uint64(i)<<16 + 2)
	}

	for i := 10; i < 20; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(1), c.get(indexCardinality))
	}
}

func TestBulkAdd(t *testing.T) {
	ra := NewRoaringArray(2)

	max := 10 << 16
	for i := 1; i <= max; i++ {
		ra.Add(uint64(i))
	}

	offset, has := ra.keys.getValue(0)
	require.True(t, has)
	c := ra.getContainer(offset)
	for i := 1; i <= max; i++ {
		require.Equal(t, uint16(i-1), c.find(uint16(i)))
	}
	t.Logf("Data size: %d\n", len(ra.data))

	dup := make([]byte, len(ra.data))
	copy(dup, ra.data)

	ra2 := FromBuffer(dup)
	offset, has = ra2.keys.getValue(0)
	require.True(t, has)
	c = ra2.getContainer(offset)
	for i := 1; i <= 1000; i++ {
		require.Equal(t, uint16(i-1), c.find(uint16(i)))
	}
}

func TestUint16(t *testing.T) {
	var x uint16
	for i := 0; i < 100000; i++ {
		prev := x
		x++
		if x <= prev {
			// This triggers when prev = 0xFFFF.
			require.Failf(t, "x<=prev", "x %d <= prev %d", x, prev)
		}
	}
}
