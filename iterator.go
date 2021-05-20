package sroar

type Iterator struct {
	index   int
	reverse bool
	arr     []uint64
}

func (r *Bitmap) NewIterator() *Iterator {
	return &Iterator{
		index: -1,
		arr:   r.ToArray(),
	}
}

func (r *Bitmap) NewReverseIterator() *Iterator {
	return &Iterator{
		index:   r.GetCardinality(),
		arr:     r.ToArray(),
		reverse: true,
	}
}

func (itr *Iterator) HasNext() bool {
	if itr.reverse {
		return itr.index > 0
	} else {
		return int(itr.index) < len(itr.arr)-1
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

type ManyItr struct {
	index int
	arr   []uint64
}

func (r *Bitmap) ManyIterator() *ManyItr {
	return &ManyItr{
		arr: r.ToArray(),
	}

}

func (itr *ManyItr) NextMany(buf []uint64) int {
	count := 0
	for i := 0; i < len(buf); i++ {
		if itr.index == len(itr.arr) {
			break
		}
		buf[i] = itr.arr[itr.index]
		itr.index++
		count++
	}
	return count
}
