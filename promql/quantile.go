// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"math"
	"slices"
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/util/almost"
)

// smallDeltaTolerance is the threshold for relative deltas between classic
// histogram buckets that will be ignored by the histogram_quantile function
// because they are most likely artifacts of floating point precision issues.
// Testing on 2 sets of real data with bugs arising from small deltas,
// the safe ranges were from:
// - 1e-05 to 1e-15
// - 1e-06 to 1e-15
// Anything to the left of that would cause non-query-sharded data to have
// small deltas ignored (unnecessary and we should avoid this), and anything
// to the right of that would cause query-sharded data to not have its small
// deltas ignored (so the problem won't be fixed).
// For context, query sharding triggers these float precision errors in Mimir.
// To illustrate, with a relative deviation of 1e-12, we need to have 1e12
// observations in the bucket so that the change of one observation is small
// enough to get ignored. With the usual observation rate even of very busy
// services, this will hardly be reached in timeframes that matters for
// monitoring.
const smallDeltaTolerance = 1e-12

// Helpers to calculate quantiles.

// excludedLabels are the labels to exclude from signature calculation for
// quantiles.
var excludedLabels = []string{
	labels.MetricName,
	labels.BucketLabel,
}

// bucket 表示经典直方图的一个桶：upperBound 为桶的上边界，count 为
// 累计观测数（该桶及所有更小桶的观测总和，即 cumulative count）。
type bucket struct {
	upperBound float64
	count      float64
}

// buckets implements sort.Interface.
// buckets 实现了 sort.Interface，支持按 upperBound 排序。
type buckets []bucket

// metricWithBuckets 将一组桶与其所属的标签集（除去 metric name 和 le
// 之外的所有标签，用于标识同一直方图）关联在一起，
// 是 histogram_quantile 函数按标签分组计算分位数时的中间结构。
type metricWithBuckets struct {
	metric  labels.Labels
	buckets buckets
}

// bucketQuantile calculates the quantile 'q' based on the given buckets. The
// buckets will be sorted by upperBound by this function (i.e. no sorting
// needed before calling this function). The quantile value is interpolated
// assuming a linear distribution within a bucket. However, if the quantile
// falls into the highest bucket, the upper bound of the 2nd highest bucket is
// returned. A natural lower bound of 0 is assumed if the upper bound of the
// lowest bucket is greater 0. In that case, interpolation in the lowest bucket
// happens linearly between 0 and the upper bound of the lowest bucket.
// However, if the lowest bucket has an upper bound less or equal 0, this upper
// bound is returned if the quantile falls into the lowest bucket.
//
// There are a number of special cases (once we have a way to report errors
// happening during evaluations of AST functions, we should report those
// explicitly):
//
// If 'buckets' has 0 observations, NaN is returned.
//
// If 'buckets' has fewer than 2 elements, NaN is returned.
//
// If the highest bucket is not +Inf, NaN is returned.
//
// If q==NaN, NaN is returned.
//
// If q<0, -Inf is returned.
//
// If q>1, +Inf is returned.
//
// We also return a bool to indicate if monotonicity needed to be forced,
// and another bool to indicate if small differences between buckets (that
// are likely artifacts of floating point precision issues) have been
// ignored.
// 经典直方图分位数：排序 → 合并同界桶 → 强制单调 → 二分定位桶 → 桶内线性插值
func bucketQuantile(q float64, buckets buckets) (float64, bool, bool) {
	if math.IsNaN(q) {
		return math.NaN(), false, false
	}
	if q < 0 {
		return math.Inf(-1), false, false
	}
	if q > 1 {
		return math.Inf(+1), false, false
	}
	slices.SortFunc(buckets, func(a, b bucket) int {
		// We don't expect the bucket boundary to be a NaN.
		if a.upperBound < b.upperBound {
			return -1
		}
		if a.upperBound > b.upperBound {
			return +1
		}
		return 0
	})
	if !math.IsInf(buckets[len(buckets)-1].upperBound, +1) {
		return math.NaN(), false, false
	}

	buckets = coalesceBuckets(buckets)
	// 修正桶计数：忽略浮点精度造成的微小差异，并强制桶计数单调递增
	forcedMonotonic, fixedPrecision := ensureMonotonicAndIgnoreSmallDeltas(buckets, smallDeltaTolerance)

	if len(buckets) < 2 {
		return math.NaN(), false, false
	}
	// 总观测数 = 最大桶（+Inf 桶）的累计计数
	observations := buckets[len(buckets)-1].count
	if observations == 0 {
		return math.NaN(), false, false
	}
	// φ 分位数对应的目标排名（第 rank 个观测）
	rank := q * observations
	// 二分查找第一个累计计数 >= rank 的桶，即分位数落入的桶
	b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })

	if b == len(buckets)-1 {
		// 分位数落入 +Inf 桶：无法插值，返回次高桶的上边界作为估计值
		return buckets[len(buckets)-2].upperBound, forcedMonotonic, fixedPrecision
	}
	if b == 0 && buckets[0].upperBound <= 0 {
		// 分位数落入上边界 <= 0 的最低桶：无法向 0 插值，直接返回其上边界
		return buckets[0].upperBound, forcedMonotonic, fixedPrecision
	}
	var (
		bucketStart float64
		bucketEnd   = buckets[b].upperBound
		count       = buckets[b].count
	)
	if b > 0 {
		// 换算为分位数所在桶内的局部值：
		// 桶下边界 = 前一桶的上边界；桶内观测数 / 桶内排名 = 减去前桶累计值
		bucketStart = buckets[b-1].upperBound
		count -= buckets[b-1].count
		rank -= buckets[b-1].count
	}
	// 假设观测在桶内线性分布，按比例插值得到分位数估计值
	return bucketStart + (bucketEnd-bucketStart)*(rank/count), forcedMonotonic, fixedPrecision
}

// histogramQuantile calculates the quantile 'q' based on the given histogram.
//
// The quantile value is interpolated assuming a linear distribution within a
// bucket.
// TODO(beorn7): Find an interpolation method that is a better fit for
// exponential buckets (and think about configurable interpolation).
//
// A natural lower bound of 0 is assumed if the histogram has only positive
// buckets. Likewise, a natural upper bound of 0 is assumed if the histogram has
// only negative buckets.
// TODO(beorn7): Come to terms if we want that.
//
// There are a number of special cases (once we have a way to report errors
// happening during evaluations of AST functions, we should report those
// explicitly):
//
// If the histogram has 0 observations, NaN is returned.
//
// If q<0, -Inf is returned.
//
// If q>1, +Inf is returned.
//
// If q is NaN, NaN is returned.
// 原生直方图（NHCB）分位数：迭代桶累积计数定位，q≥0.5 时用反向迭代器优化
func histogramQuantile(q float64, h *histogram.FloatHistogram) float64 {
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}

	if h.Count == 0 || math.IsNaN(q) {
		return math.NaN()
	}

	var (
		bucket histogram.Bucket[float64]
		count  float64
		it     histogram.BucketIterator[float64]
		rank   float64
	)

	// if there are NaN observations in the histogram (h.Sum is NaN), use the forward iterator
	// if the q < 0.5, use the forward iterator
	// if the q >= 0.5, use the reverse iterator
	if math.IsNaN(h.Sum) || q < 0.5 {
		it = h.AllBucketIterator()
		rank = q * h.Count
	} else {
		it = h.AllReverseBucketIterator()
		rank = (1 - q) * h.Count
	}

	for it.Next() {
		bucket = it.At()
		count += bucket.Count
		if count >= rank {
			break
		}
	}
	if bucket.Lower < 0 && bucket.Upper > 0 {
		switch {
		case len(h.NegativeBuckets) == 0 && len(h.PositiveBuckets) > 0:
			// The result is in the zero bucket and the histogram has only
			// positive buckets. So we consider 0 to be the lower bound.
			bucket.Lower = 0
		case len(h.PositiveBuckets) == 0 && len(h.NegativeBuckets) > 0:
			// The result is in the zero bucket and the histogram has only
			// negative buckets. So we consider 0 to be the upper bound.
			bucket.Upper = 0
		}
	}
	// Due to numerical inaccuracies, we could end up with a higher count
	// than h.Count. Thus, make sure count is never higher than h.Count.
	if count > h.Count {
		count = h.Count
	}
	// We could have hit the highest bucket without even reaching the rank
	// (this should only happen if the histogram contains observations of
	// the value NaN), in which case we simply return the upper limit of the
	// highest explicit bucket.
	if count < rank {
		return bucket.Upper
	}

	// NaN observations increase h.Count but not the total number of
	// observations in the buckets. Therefore, we have to use the forward
	// iterator to find percentiles. We recognize histograms containing NaN
	// observations by checking if their h.Sum is NaN.
	if math.IsNaN(h.Sum) || q < 0.5 {
		rank -= count - bucket.Count
	} else {
		rank = count - rank
	}

	// TODO(codesome): Use a better estimation than linear.
	return bucket.Lower + (bucket.Upper-bucket.Lower)*(rank/bucket.Count)
}

// histogramFraction calculates the fraction of observations between the
// provided lower and upper bounds, based on the provided histogram.
//
// histogramFraction is in a certain way the inverse of histogramQuantile.  If
// histogramQuantile(0.9, h) returns 123.4, then histogramFraction(-Inf, 123.4, h)
// returns 0.9.
//
// The same notes (and TODOs) with regard to interpolation and assumptions about
// the zero bucket boundaries apply as for histogramQuantile.
//
// Whether either boundary is inclusive or exclusive doesn’t actually matter as
// long as interpolation has to be performed anyway. In the case of a boundary
// coinciding with a bucket boundary, the inclusive or exclusive nature of the
// boundary determines the exact behavior of the threshold. With the current
// implementation, that means that lower is exclusive for positive values and
// inclusive for negative values, while upper is inclusive for positive values
// and exclusive for negative values.
//
// Special cases:
//
// If the histogram has 0 observations, NaN is returned.
//
// Use a lower bound of -Inf to get the fraction of all observations below the
// upper bound.
//
// Use an upper bound of +Inf to get the fraction of all observations above the
// lower bound.
//
// If lower or upper is NaN, NaN is returned.
//
// If lower >= upper and the histogram has at least 1 observation, zero is returned.
func histogramFraction(lower, upper float64, h *histogram.FloatHistogram) float64 {
	if h.Count == 0 || math.IsNaN(lower) || math.IsNaN(upper) {
		return math.NaN()
	}
	if lower >= upper {
		return 0
	}

	var (
		rank, lowerRank, upperRank float64
		lowerSet, upperSet         bool
		it                         = h.AllBucketIterator()
	)
	for it.Next() {
		b := it.At()
		if b.Lower < 0 && b.Upper > 0 {
			switch {
			case len(h.NegativeBuckets) == 0 && len(h.PositiveBuckets) > 0:
				// This is the zero bucket and the histogram has only
				// positive buckets. So we consider 0 to be the lower
				// bound.
				b.Lower = 0
			case len(h.PositiveBuckets) == 0 && len(h.NegativeBuckets) > 0:
				// This is in the zero bucket and the histogram has only
				// negative buckets. So we consider 0 to be the upper
				// bound.
				b.Upper = 0
			}
		}
		if !lowerSet && b.Lower >= lower {
			lowerRank = rank
			lowerSet = true
		}
		if !upperSet && b.Lower >= upper {
			upperRank = rank
			upperSet = true
		}
		if lowerSet && upperSet {
			break
		}
		if !lowerSet && b.Lower < lower && b.Upper > lower {
			lowerRank = rank + b.Count*(lower-b.Lower)/(b.Upper-b.Lower)
			lowerSet = true
		}
		if !upperSet && b.Lower < upper && b.Upper > upper {
			upperRank = rank + b.Count*(upper-b.Lower)/(b.Upper-b.Lower)
			upperSet = true
		}
		if lowerSet && upperSet {
			break
		}
		rank += b.Count
	}
	if !lowerSet || lowerRank > h.Count {
		lowerRank = h.Count
	}
	if !upperSet || upperRank > h.Count {
		upperRank = h.Count
	}

	return (upperRank - lowerRank) / h.Count
}

// coalesceBuckets merges buckets with the same upper bound.
//
// The input buckets must be sorted.
func coalesceBuckets(buckets buckets) buckets {
	last := buckets[0]
	i := 0
	for _, b := range buckets[1:] {
		if b.upperBound == last.upperBound {
			last.count += b.count
		} else {
			buckets[i] = last
			last = b
			i++
		}
	}
	buckets[i] = last
	return buckets[:i+1]
}

// The assumption that bucket counts increase monotonically with increasing
// upperBound may be violated during:
//
//   - Circumstances where data is already inconsistent at the target's side.
//   - Ingestion via the remote write receiver that Prometheus implements.
//   - Optimisation of query execution where precision is sacrificed for other
//     benefits, not by Prometheus but by systems built on top of it.
//   - Circumstances where floating point precision errors accumulate.
//
// Monotonicity is usually guaranteed because if a bucket with upper bound
// u1 has count c1, then any bucket with a higher upper bound u > u1 must
// have counted all c1 observations and perhaps more, so that c >= c1.
//
// bucketQuantile depends on that monotonicity to do a binary search for the
// bucket with the φ-quantile count, so breaking the monotonicity
// guarantee causes bucketQuantile() to return undefined (nonsense) results.
//
// As a somewhat hacky solution, we first silently ignore any numerically
// insignificant (relative delta below the requested tolerance and likely to
// be from floating point precision errors) differences between successive
// buckets regardless of the direction. Then we calculate the "envelope" of
// the histogram buckets, essentially removing any decreases in the count
// between successive buckets.
//
// We return a bool to indicate if this monotonicity was forced or not, and
// another bool to indicate if small deltas were ignored or not.
// 忽略微小浮点误差 + 强制桶计数单调递增（二分查找的前提）
func ensureMonotonicAndIgnoreSmallDeltas(buckets buckets, tolerance float64) (bool, bool) {
	var forcedMonotonic, fixedPrecision bool
	prev := buckets[0].count
	for i := 1; i < len(buckets); i++ {
		curr := buckets[i].count // Assumed always positive.
		if curr == prev {
			// No correction needed if the counts are identical between buckets.
			continue
		}
		if almost.Equal(prev, curr, tolerance) {
			// Silently correct numerically insignificant differences from floating
			// point precision errors, regardless of direction.
			// Do not update the 'prev' value as we are ignoring the difference.
			buckets[i].count = prev
			fixedPrecision = true
			continue
		}
		if curr < prev {
			// Force monotonicity by removing any decreases regardless of magnitude.
			// Do not update the 'prev' value as we are ignoring the decrease.
			buckets[i].count = prev
			forcedMonotonic = true
			continue
		}
		prev = curr
	}
	return forcedMonotonic, fixedPrecision
}

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
// 普通样本向量的分位数：排序后按加权平均插值（quantile() 函数用）
func quantile(q float64, values vectorByValueHeap) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	sort.Sort(values)

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)].F*(1-weight) + values[int(upperIndex)].F*weight
}
