package org.apache.pinot.segment.local.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SamplingCardinalityEstimatorTest {

  @Test(dataProvider = "uniformDistributionParams")
  public void testUniformDistribution(int numValues, int valueMultiplier, int expectedActualDistinctValues, int expectedEstimatedDistinctValues) {
    UniformRealDistribution uniformRealDistribution = new UniformRealDistribution(new Well19937c(42), 0.001, 1.000);
    int[] values = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (int) (valueMultiplier * uniformRealDistribution.sample());
    }
    SamplingCardinalityEstimator estimator = new SamplingCardinalityEstimator();
    Set<Integer> uniqueValues = new HashSet<>();
    for (int value : values) {
      estimator.add(value);
      uniqueValues.add(value);
    }

    assertEquals(uniqueValues.size(), expectedActualDistinctValues);
    assertEquals(estimator.getEstimatedCardinality(), expectedEstimatedDistinctValues);
  }

  @DataProvider
  public static Object[][] uniformDistributionParams() {
    int numValues = 1000000;
    List<Integer> valueMultiplier = Arrays.asList(10, 100, 1000, 10000, 100000, numValues);
    List<Integer> actualDistinctValues = Arrays.asList(10, 100, 999, 9990, 99896, 631572);
    List<Integer> estimatedDistinctValues = Arrays.asList(10, 100, 999, 10333, 146540, 239332);

    return IntStream.range(0, valueMultiplier.size()).mapToObj(
            i -> new Object[]{numValues, valueMultiplier.get(i), actualDistinctValues.get(i),
                estimatedDistinctValues.get(i)})
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "exponentialDistributionParams")
  public void testExponentialDistribution(int numValues, int valueMultiplier, int expectedActualDistinctValues, int expectedEstimatedDistinctValues) {
    ExponentialDistribution exponentialDistribution = new ExponentialDistribution(new Well19937c(42), 1.0);
    int[] values = new int[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = (int) (valueMultiplier * exponentialDistribution.sample());
    }
    Arrays.sort(values);
    SamplingCardinalityEstimator estimator = new SamplingCardinalityEstimator();
    Set<Integer> uniqueValues = new HashSet<>();
    for (int value : values) {
      estimator.add(value);
      uniqueValues.add(value);
    }
    assertEquals(uniqueValues.size(), expectedActualDistinctValues);
    assertEquals(estimator.getEstimatedCardinality(), expectedEstimatedDistinctValues);
  }


  @DataProvider
  public static Object[][] exponentialDistributionParams() {
    int numValues = 1000000;
    List<Integer> valueMultiplier = Arrays.asList(10, 100, 1000, 10000, 100000, numValues);
    List<Integer> actualDistinctValues = Arrays.asList(125, 984, 7504, 51779, 287741, 796923);
    List<Integer> estimatedDistinctValues = Arrays.asList(116, 965, 7575, 53780, 194197, 245928);

    return IntStream.range(0, valueMultiplier.size()).mapToObj(
            i -> new Object[]{numValues, valueMultiplier.get(i), actualDistinctValues.get(i),
                estimatedDistinctValues.get(i)})
        .toArray(Object[][]::new);
  }
}
