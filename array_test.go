package roar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func fill(c []uint16, b uint16) {
	for i := range c[startIdx:] {
		c[i+int(startIdx)] = b
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
	ra := NewBitmap()

	offset := ra.newContainer(128)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(128), getSize(ra.data[offset:]))
	require.Equal(t, uint16(0), c[indexCardinality])

	fill(c, 0xFF)
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}

	offset2 := ra.newContainer(64) // Add a second container.
	c2 := ra.getContainer(offset2)
	require.Equal(t, uint16(64), getSize(ra.data[offset2:]))
	fill(c2, 0xEE)

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset)
	offset2 += 128

	// Check if the second container is correct.
	c2 = ra.getContainer(offset2)
	require.Equal(t, uint16(64), getSize(ra.data[offset2:]))
	require.Equal(t, 32, len(c2))
	for _, val := range c2[startIdx:] {
		require.Equal(t, uint16(0xEE), val)
	}

	// Check if the first container is correct.
	c = ra.getContainer(offset)
	require.Equal(t, uint16(256), getSize(ra.data[offset:]))
	require.Equal(t, 128, len(c))
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}
}

func TestKey(t *testing.T) {
	ra := NewBitmap()
	for i := 1; i <= 10; i++ {
		ra.Add(uint64(i))
	}

	off, has := ra.keys.getValue(0)
	require.True(t, has)
	c := ra.getContainer(off)
	require.Equal(t, uint16(10), c[indexCardinality])

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
		require.Equal(t, uint16(2), c[indexCardinality])
	}

	// Do add in the reverse order.
	for i := 19; i >= 10; i-- {
		ra.Add(uint64(i)<<16 + 2)
	}

	for i := 10; i < 20; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(1), c[indexCardinality])
	}
}

func TestEdgeCase(t *testing.T) {
	ra := NewBitmap()

	require.True(t, ra.Add(65536))
	require.True(t, ra.Has(65536))
}

func TestBulkAdd(t *testing.T) {
	ra := NewBitmap()

	max := uint64(10 << 16)
	for i := uint64(1); i <= max; i++ {
		ra.Add(uint64(i))
		//	t.Logf("Added: %d\n", i)
	}

	_, has := ra.keys.getValue(0)
	require.True(t, has)
	for i := uint64(1); i <= max; i++ {
		require.Truef(t, ra.Has(i), "i=%d", i)
	}
	t.Logf("Data size: %d\n", len(ra.data))

	dup := make([]byte, len(ra.data))
	copy(dup, ra.data)

	ra2 := FromBuffer(dup)
	for i := uint64(1); i <= max; i++ {
		require.True(t, ra2.Has(i))
	}
}

func TestUint16(t *testing.T) {
	var x uint16
	for i := 0; i < 100000; i++ {
		prev := x
		x++
		if x <= prev {
			// This triggers when prev = 0xFFFF.
			// require.Failf(t, "x<=prev", "x %d <= prev %d", x, prev)
		}
	}
}
