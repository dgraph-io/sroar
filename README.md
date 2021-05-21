# sroar: Serialized Roaring Bitmaps

sroar is a re-written version of Roaring Bitmaps in Go, with the aim to have
equality between in-memory representation and on-disk representation. An
sroar.Bitmap does not need to be marshalled or unmarshalled, as the underlying
represetation is a byte slice. Therefore, it can be written to disk, brought to
memory, or shipped over the network immediately. This is needed in [Dgraph][], where
we need to deal with lots of bitmaps.

sroar only implements array and bitmap containers. It does NOT implement run
containers, which is an optimization that RoaringBitmaps has. Despite that, it
outperforms RoaringBitmaps as shown in the Benchmarks section.

[Dgraph]: https://github.com/dgraph-io/dgraph
[Roaring]: https://github.com/RoaringBitmap/roaring

The code borrows concepts and code from [RoaringBitmaps][Roaring].

## Benchmarks

The benchmarks were run:
- Using real data set as described in [RoaringBitmaps][Roaring].
- Only on the 64-bit version of roaring bitmaps (roaring64).
- Only on `FastOr`, which is the more expensive operation than `And` or
    equivalent.
- On AMD Ryzen Threadripper 2950X 16-Core Processor.
- Using Go benchmarks serially.

Based on the benchmarks, sroar is:
- 4x faster (-75% p50 based on benchstat), uses
- 4x less memory (-75% p50), and does
- 25x fewer allocations (-96% p50).

The command used to run these was:
```
$ go test -bench BenchmarkRealDataFastOr --run=XXX --count=5 --benchmem
```

```
name  CPU                                    old time/op    new time/op    delta
RealDataFastOr/census-income-32             1.70ms ± 3%    1.65ms ± 3%      ~     (p=0.151 n=5+5)
RealDataFastOr/census1881-32                 302ms ± 2%       3ms ± 2%   -99.02%  (p=0.008 n=5+5)
RealDataFastOr/dimension_003-32             50.4ms ± 1%    13.7ms ± 2%   -72.85%  (p=0.008 n=5+5)
RealDataFastOr/dimension_008-32             10.0ms ± 2%     4.4ms ± 4%   -56.12%  (p=0.008 n=5+5)
RealDataFastOr/dimension_033-32             55.0ms ± 3%     3.5ms ± 3%   -93.56%  (p=0.008 n=5+5)
RealDataFastOr/uscensus2000-32               556µs ± 2%     780µs ± 2%   +40.33%  (p=0.008 n=5+5)
RealDataFastOr/weather_sept_85-32           2.28ms ± 2%    6.35ms ± 2%  +178.37%  (p=0.008 n=5+5)
RealDataFastOr/wikileaks-noquotes-32        76.5ms ± 1%     1.1ms ± 1%   -98.60%  (p=0.008 n=5+5)
RealDataFastOr/census-income_srt-32          260µs ± 4%    1545µs ± 1%  +494.29%  (p=0.008 n=5+5)
RealDataFastOr/census1881_srt-32            36.8ms ± 3%     3.0ms ± 2%   -91.82%  (p=0.008 n=5+5)
RealDataFastOr/weather_sept_85_srt-32       6.13ms ± 3%    4.10ms ± 2%   -33.09%  (p=0.008 n=5+5)
RealDataFastOr/wikileaks-noquotes_srt-32    34.8ms ± 5%     1.1ms ± 2%   -96.87%  (p=0.008 n=5+5)

name  MEM_BYTES                                old alloc/op   new alloc/op   delta
RealDataFastOr/census-income-32              187kB ± 0%      70kB ± 0%   -62.82%  (p=0.008 n=5+5)
RealDataFastOr/census1881-32                 585MB ± 0%       1MB ± 0%   -99.75%  (p=0.008 n=5+5)
RealDataFastOr/dimension_003-32             7.78MB ± 0%    1.28MB ± 0%   -83.60%  (p=0.008 n=5+5)
RealDataFastOr/dimension_008-32              537kB ± 0%     162kB ± 0%   -69.88%  (p=0.008 n=5+5)
RealDataFastOr/dimension_033-32             1.10MB ± 0%    1.28MB ± 0%   +15.96%  (p=0.008 n=5+5)
RealDataFastOr/uscensus2000-32               169kB ± 0%     240kB ± 0%   +41.99%  (p=0.008 n=5+5)
RealDataFastOr/weather_sept_85-32            169kB ± 0%     293kB ± 0%   +73.00%  (p=0.008 n=5+5)
RealDataFastOr/wikileaks-noquotes-32        76.3MB ± 0%     0.6MB ± 0%   -99.23%  (p=0.008 n=5+5)
RealDataFastOr/census-income_srt-32         99.1kB ± 0%    69.6kB ± 0%   -29.75%  (p=0.008 n=5+5)
RealDataFastOr/census1881_srt-32            15.3MB ± 0%     1.5MB ± 0%   -90.31%  (p=0.008 n=5+5)
RealDataFastOr/weather_sept_85_srt-32        375kB ± 0%     293kB ± 0%   -21.91%  (p=0.008 n=5+5)
RealDataFastOr/wikileaks-noquotes_srt-32    22.8MB ± 0%     0.6MB ± 0%   -97.43%  (p=0.008 n=5+5)

name   MEM_ALLOCS                              old allocs/op  new allocs/op  delta
RealDataFastOr/census-income-32                271 ± 0%         9 ± 0%   -96.68%  (p=0.008 n=5+5)
RealDataFastOr/census1881-32                 54.5k ± 0%      0.0k ± 0%   -99.95%  (p=0.029 n=4+4)
RealDataFastOr/dimension_003-32              4.57k ± 0%     0.03k ± 2%   -99.44%  (p=0.008 n=5+5)
RealDataFastOr/dimension_008-32                704 ± 0%        23 ± 0%   -96.73%  (p=0.008 n=5+5)
RealDataFastOr/dimension_033-32              4.33k ± 0%     0.03k ± 0%   -99.40%  (p=0.000 n=5+4)
RealDataFastOr/uscensus2000-32               1.75k ± 0%     0.06k ± 0%   -96.85%  (p=0.000 n=5+4)
RealDataFastOr/weather_sept_85-32             81.0 ± 0%      14.0 ± 0%   -82.72%  (p=0.008 n=5+5)
RealDataFastOr/wikileaks-noquotes-32         39.2k ± 0%      0.0k ± 0%      ~     (p=0.079 n=4+5)
RealDataFastOr/census-income_srt-32           40.0 ± 0%       9.0 ± 0%   -77.50%  (p=0.008 n=5+5)
RealDataFastOr/census1881_srt-32             29.7k ± 0%      0.0k ± 0%   -99.91%  (p=0.008 n=5+5)
RealDataFastOr/weather_sept_85_srt-32          248 ± 0%        14 ± 0%   -94.35%  (p=0.000 n=5+4)
RealDataFastOr/wikileaks-noquotes_srt-32     6.06k ± 0%     0.02k ± 0%   -99.74%  (p=0.008 n=5+5)
```
