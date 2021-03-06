package roar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainer(t *testing.T) {
	ra := roaringArray(make([]byte, 8))

	offset := ra.newContainer(128)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(128), c.get(indexSize))
	require.Equal(t, uint16(0), c.get(indexCardinality))

	for i := range c.data() {
		c.data()[i] = 0xFF
	}
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

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset, 128)
	offset2 += 128

	c2 = ra.getContainer(offset2)
	require.Equal(t, uint16(64), c2.get(indexSize))
	require.Equal(t, 28, len(c2.data()))

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
