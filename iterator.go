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
