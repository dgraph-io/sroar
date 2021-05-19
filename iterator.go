package sroar

// TODO(Ahsan): Re-write the iterator in an optimal way. This implementation is just to make sroar
// functionality wise complete. So that it can be imported to dgraph and checked for correctness.
type Iterator struct {
	r       *Bitmap
	reverse bool

	arr     []uint16
	arrIdx  int
	keyIdx  int
	key     uint64
	numKeys int
}

func (r *Bitmap) NewIterator() *Iterator {
	itr := &Iterator{
		r:       r,
		arrIdx:  -1,
		key:     r.keys.key(0),
		numKeys: r.keys.numKeys(),
	}

	off := itr.r.keys.val(0)
	c := itr.r.getContainer(off)
	switch c[indexType] {
	case typeArray:
		itr.arr = array(c).all()
	case typeBitmap:
		itr.arr = bitmap(c).ToArray()
	}
	return itr
}

func (r *Bitmap) NewReverseIterator() *Iterator {
	n := r.keys.numKeys()
	assert(n > 0)
	itr := &Iterator{
		r:       r,
		key:     r.keys.key(n - 1),
		keyIdx:  n - 1,
		numKeys: n,
		reverse: true,
	}
	off := itr.r.keys.val(n - 1)
	c := itr.r.getContainer(off)
	switch c[indexType] {
	case typeArray:
		itr.arr = array(c).all()
	case typeBitmap:
		itr.arr = bitmap(c).ToArray()
	}
	itr.arrIdx = len(itr.arr)
	return itr
}

func (itr *Iterator) HasNext() bool {
	if itr.reverse {
		if itr.arrIdx > 0 || itr.keyIdx > 0 {
			return true
		}
		return false
	} else {
		if itr.arrIdx < len(itr.arr)-1 || itr.keyIdx < itr.numKeys-1 {
			return true
		}
		return false
	}
}

func (itr *Iterator) Next() uint64 {
	if !itr.reverse {
		if itr.arrIdx < len(itr.arr)-1 {
			itr.arrIdx++
		} else if itr.keyIdx < itr.numKeys-1 {
			itr.keyIdx++
			itr.key = itr.r.keys.key(itr.keyIdx)
			off := itr.r.keys.val(itr.keyIdx)
			c := itr.r.getContainer(off)
			switch c[indexType] {
			case typeArray:
				itr.arr = array(c).all()
			case typeBitmap:
				itr.arr = bitmap(c).ToArray()
			}
			itr.arrIdx = 0
			// update arr
		} else {
			panic("No more element")
		}
	} else {
		if itr.arrIdx > 0 {
			itr.arrIdx--
		} else if itr.keyIdx > 0 {
			itr.keyIdx--
			itr.key = itr.r.keys.key(itr.keyIdx)
			off := itr.r.keys.val(itr.keyIdx)
			c := itr.r.getContainer(off)
			switch c[indexType] {
			case typeArray:
				itr.arr = array(c).all()
			case typeBitmap:
				itr.arr = bitmap(c).ToArray()
			}
			itr.arrIdx = len(itr.arr) - 1
			// update arr
		} else {
			panic("No more element")
		}
	}
	return itr.key | uint64(itr.arr[itr.arrIdx])
}

func (itr *Iterator) Val() uint64 {
	return itr.key | uint64(itr.arr[itr.arrIdx])
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *Iterator) AdvanceIfNeeded(minval uint64) {
	for itr.HasNext() && itr.Val() < minval {
		itr.Next()
	}
}
