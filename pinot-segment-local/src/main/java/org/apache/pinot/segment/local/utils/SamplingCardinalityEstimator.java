package org.apache.pinot.segment.local.utils;

import com.dynatrace.hash4j.hashing.HashFunnel;
import com.dynatrace.hash4j.hashing.Hasher64;
import com.dynatrace.hash4j.hashing.Hashing;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.Random;


/**
 * Simple error estimator based on the GEE (Guaranteed Error Estimator) algorithm from the paper
 * Towards Estimation Error Guarantees for Distinct Values, Charikar et al. 2002.
 * <p>
 * The GEE algorithm is a simple sampling algorithm for estimating the number of distinct values, and rapidly
 * converges to the true value given a large enough sample size. The algorithm is based on the observation that
 * the probability of a value being in the sample is proportional to the number of times it has been seen so far.
 * <p>
 * It provides good error guarantees, and the error tends to skew towards the upper bound. If duplication factor
 * of values is near 1, the estimated cardinality may be underestimated by a factor of 2-3x.
 */
public class SamplingCardinalityEstimator {

  // sample 6.4% of values, generally a good trade-off between accuracy and performance
  private static final double DEFAULT_SAMPLING_RATIO = 0.064;

  // consider using Object.hashCode for short strings
  private static final Hasher64 DEFAULT_HASHER = Hashing.wyhashFinal4();
  private int _doc = 0;
  private int _samples = 0;
  // map of value hash to frequency of the hash
  private final Int2IntOpenHashMap frequencies = new Int2IntOpenHashMap();
  private final Random random;

  public SamplingCardinalityEstimator() {
    random = new Random(42);
  }

  /**
   * Add the value to the estimator
   */
  public void add(Object value) {
    _doc++;
    if (random.nextDouble() < DEFAULT_SAMPLING_RATIO) {
      _samples++;
      hashObject(value).ifPresent(hash -> frequencies.compute(hash.intValue(), (k, v) -> v == null ? 1 : v + 1));
    }
  }

  /**
   * Returns the estimated cardinality of the values seen so far.
   */
  public int getEstimatedCardinality() {
    int f1 = 0;
    for (int freq : frequencies.values()) {
      if (freq == 1) {
        f1++;
      }
    }
    return (int) (frequencies.size() + (Math.sqrt((double) _doc / _samples) - 1) * f1);
  }

  /**
   * Heuristic correction for estimated cardinality, based on the observation that GEE tends to underestimate the
   * cardinality when there are minimal duplicates. This magic number correction relies on the current fixed
   * sampling ratio, and must be updated if sampling ratio is changed or made configurable.
   *
   * TODO: probably remove this, it can be handled per use-case
   */
  public int getCorrectedEstimatedCardinality() {
    int estimatedCardinality = getEstimatedCardinality();
    if (estimatedCardinality < _doc / 10) {
      return estimatedCardinality;
    }

    double x0 = -2.5e4;
    double x1 = 4.1e0;
    double x2 = -4.4e-5;
    double x3 = 1.7e-10;
    return (int) (x0 + x1 * estimatedCardinality + x2 * Math.pow(estimatedCardinality, 2) + x3 * Math.pow(
        estimatedCardinality, 3));
  }

  private static Optional<Integer> hashObject(Object obj) {
    return Optional.ofNullable(obj)
        .map(o -> DEFAULT_HASHER.hashToInt(o, OBJECT_FUNNEL));
  }

  private static final HashFunnel<Object> OBJECT_FUNNEL = (o, hashSink) -> {
    if (o instanceof Integer) {
      hashSink.putInt((Integer) o);
    } else if (o instanceof Long) {
      hashSink.putLong((Long) o);
    } else if (o instanceof Float) {
      hashSink.putFloat((Float) o);
    } else if (o instanceof Double) {
      hashSink.putDouble((Double) o);
    } else if (o instanceof BigDecimal) {
      hashSink.putString(o.toString());
    } else if (o instanceof String) {
      hashSink.putString((String) o);
    } else if (o instanceof byte[]) {
      hashSink.putBytes((byte[]) o);
    } else {
      throw new IllegalArgumentException(
          "Unrecognised input type for SamplingCardinalityEstimator: " + o.getClass().getSimpleName());
    }
  };
}
