# sroar: Serialized Roaring Bitmaps

sroar is a re-written version of Roaring Bitmaps in Go, with the aim to have
equality between in-memory representation and on-disk representation. An
sroar.Bitmap does not need to be marshalled or unmarshalled, as the underlying
represetation is a byte slice. Therefore, it can be written to disk, brought to
memory, or shipped over the network immediately. This is needed in [Dgraph][], where
we need to deal with lots of bitmaps.

[Dgraph]: https://github.com/dgraph-io/dgraph
[Roaring]: https://github.com/RoaringBitmap/roaring

The code borrows concepts and code from [RoaringBitmaps][Roaring].

