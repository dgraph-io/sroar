package sroar

// TODO(Ahsan): Re-write the iterator in an optimal way. This implementation is just to make sroar
// functionality wise complete. So that it can be imported to dgraph and checked for correctness.
type Itr struct {
	r       *Bitmap
	index   int
	reverse bool
}

func (r *Bitmap) Iterator() *Itr {
	return &Itr{
		r:     r,
		index: -1,
	}
}

func (r *Bitmap) ReverseIterator() *Itr {
	return &Itr{
		r:       r,
		index:   r.GetCardinality(),
		reverse: true,
	}
}

func (itr *Itr) HasNext() bool {
	if itr.reverse {
		return itr.index > 0
	} else {
		return int(itr.index) < itr.r.GetCardinality()-1
	}
}

func (itr *Itr) Next() uint64 {
	if itr.reverse {
		itr.index--

	} else {
		itr.index++
	}
	return itr.r.ToArray()[itr.index]
}

func (itr *Itr) Val() uint64 {
	return itr.r.ToArray()[itr.index]
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *Itr) AdvanceIfNeeded(minval uint64) {
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
