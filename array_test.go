package roar

import (
	"encoding/hex"
	"fmt"
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

	t.Logf("data:\n%s\n", hex.Dump(data))
}

func TestContainer(t *testing.T) {
	ra := NewRoaringArray(2)

	offset := ra.newContainer(128)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(128), c.get(indexSize))
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
	require.Equal(t, uint16(64), c2.get(indexSize))
	fill(c2, 0xEE)

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset, 128)
	offset2 += 128

	// Check if the second container is correct.
	c2 = ra.getContainer(offset2)
	require.Equal(t, uint16(64), c2.get(indexSize))
	require.Equal(t, 28, len(c2.data()))
	for _, val := range c2.data() {
		require.Equal(t, uint16(0xEE), val)
	}

	// Check if the first container is correct.
	c = ra.getContainer(offset)
	require.Equal(t, uint16(256), c.get(indexSize))
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
	t.Logf("Num keys: %d\n", ra.keys.numKeys())

	for i := 0; i < 10; i++ {
		ra.Add(uint64(i))
	}
	fmt.Println()
	fmt.Println()
	fmt.Println()

	off, has := ra.keys.getValue(0)
	require.True(t, has)
	t.Logf("Got offset: %d\n", off)
	c := ra.getContainer(off)
	require.Equal(t, uint16(10), c.get(indexCardinality))

	// Create 10 containers
	for i := 0; i < 10; i++ {
		ra.Add(uint64(i)<<16 + 1)
	}
}
