package sroar

type Iterator struct {
	r       *Bitmap
	index   int
	reverse bool
	arr     []uint64
}

func (r *Bitmap) NewIterator() *Iterator {
	return &Iterator{
		r:     r,
		index: -1,
		arr:   r.ToArray(),
	}
}

func (r *Bitmap) NewReverseIterator() *Iterator {
	return &Iterator{
		r:       r,
		index:   r.GetCardinality(),
		arr:     r.ToArray(),
		reverse: true,
	}
}

func (itr *Iterator) HasNext() bool {
	if itr.reverse {
		return itr.index > 0
	} else {
		return int(itr.index) < itr.r.GetCardinality()-1
	}
}

func (itr *Iterator) Next() uint64 {
	if itr.reverse {
		itr.index--

	} else {
		itr.index++
	}
	return itr.arr[itr.index]
}

func (itr *Iterator) Val() uint64 {
	return itr.arr[itr.index]
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *Iterator) AdvanceIfNeeded(minval uint64) {
	if itr.index < 0 {
		return
	}
	for itr.Val() < minval {
		if itr.HasNext() {
			itr.Next()
		} else {
			break
		}
	}
}

// IterateOpt is ~20 times faster compared to Iterator. But if has an issue when it runs on a
// bitmap which contains containers with no elements.
type IteratorOpt struct {
	r       *Bitmap
	reverse bool

	arr     []uint16
	arrIdx  int
	keyIdx  int
	key     uint64
	numKeys int
}

func (r *Bitmap) NewIteratorOpt() *IteratorOpt {
	itr := &IteratorOpt{
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
		itr.arr = bitmap(c).all()
	}
	return itr
}

func (r *Bitmap) NewReverseIteratorOpt() *IteratorOpt {
	n := r.keys.numKeys()
	assert(n > 0)
	itr := &IteratorOpt{
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
		itr.arr = bitmap(c).all()
	}
	itr.arrIdx = len(itr.arr)
	return itr
}

func (itr *IteratorOpt) HasNext() bool {
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

func (itr *IteratorOpt) Next() uint64 {
	if !itr.reverse {
		// current container has more elements
		if itr.arrIdx < len(itr.arr)-1 {
			itr.arrIdx++
		} else if itr.keyIdx < itr.numKeys-1 {
			// current container is exhausted, go to next one.
			itr.keyIdx++
			itr.key = itr.r.keys.key(itr.keyIdx)
			off := itr.r.keys.val(itr.keyIdx)
			c := itr.r.getContainer(off)

			switch c[indexType] {
			case typeArray:
				itr.arr = array(c).all()
			case typeBitmap:
				itr.arr = bitmap(c).all()
			}
			itr.arrIdx = 0
			// update arr
		} else {
			panic("No more element")
		}
	} else {
		// current container has more elements
		if itr.arrIdx > 0 {
			itr.arrIdx--
		} else if itr.keyIdx > 0 {
			// current container is exhausted, go to next one.
			itr.keyIdx--
			itr.key = itr.r.keys.key(itr.keyIdx)
			off := itr.r.keys.val(itr.keyIdx)
			c := itr.r.getContainer(off)

			switch c[indexType] {
			case typeArray:
				itr.arr = array(c).all()
			case typeBitmap:
				itr.arr = bitmap(c).all()
			}
			itr.arrIdx = len(itr.arr) - 1
			// update arr
		} else {
			panic("No more element")
		}
	}
	return itr.key | uint64(itr.arr[itr.arrIdx])
}

func (itr *IteratorOpt) Val() uint64 {
	return itr.key | uint64(itr.arr[itr.arrIdx])
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *IteratorOpt) AdvanceIfNeeded(minval uint64) {
	for itr.HasNext() && itr.Val() < minval {
		itr.Next()
	}
}
