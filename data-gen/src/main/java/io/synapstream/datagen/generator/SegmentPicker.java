package io.synapstream.datagen.generator;

import java.util.Arrays;
import java.util.SplittableRandom;
import java.util.function.IntPredicate;

/**
 * Picks segment indexes at random, each with probability proportional to its weight. A segment that turns out to be
 * unavailable (its walk has used the whole pool) gets weight 0 from then on.
 */
final class SegmentPicker {

    private final double[] weights;
    private final double[] cumulative;
    private double total;

    SegmentPicker(double[] weights) {
        this.weights = weights.clone();
        this.cumulative = new double[weights.length];
        rebuild();
    }

    /** Log-normal weights: {@code exp(skew * N(0,1))} per segment. */
    static SegmentPicker logNormal(int segments, double skew, SplittableRandom random) {
        double[] weights = new double[segments];
        for (int s = 0; s < segments; s++) {
            weights[s] = Math.exp(skew * random.nextGaussian());
        }
        return new SegmentPicker(weights);
    }

    /** Weights proportional to {@code counts}; equal weights if every count is zero. */
    static SegmentPicker proportional(long[] counts) {
        double[] weights = new double[counts.length];
        boolean any = false;
        for (int s = 0; s < counts.length; s++) {
            weights[s] = counts[s];
            any |= counts[s] > 0;
        }
        if (!any) {
            Arrays.fill(weights, 1);
        }
        return new SegmentPicker(weights);
    }

    int pick(SplittableRandom random, IntPredicate available) {
        while (true) {
            if (total <= 0) {
                throw new IllegalStateException("Every segment has used the whole user pool");
            }
            int index = Arrays.binarySearch(cumulative, random.nextDouble() * total);
            int segment = index >= 0 ? index + 1 : -index - 1;
            // Guard against landing past the end through rounding, or on a zero-weight segment at a boundary.
            segment = Math.min(segment, weights.length - 1);
            if (weights[segment] > 0 && available.test(segment)) {
                return segment;
            }
            if (weights[segment] > 0) {
                weights[segment] = 0;
                rebuild();
            }
        }
    }

    private void rebuild() {
        double sum = 0;
        for (int s = 0; s < weights.length; s++) {
            sum += weights[s];
            cumulative[s] = sum;
        }
        total = sum;
    }
}
