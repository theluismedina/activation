package io.synapstream.datagen.generator;

/**
 * A full snapshot generated from scratch.
 *
 * @param userPoolSize     people are drawn from ids {@code [0, userPoolSize)}, shared by every segment
 * @param segmentSizeSkew  spread of segment sizes: each segment's expected size is proportional to
 *                         {@code exp(segmentSizeSkew * N(0,1))} (log-normal). 0 makes every segment the same size;
 *                         around 1.2 gives a long tail of a few very large segments and many small ones
 * @param seed             RNG seed, or {@code null} for a random one
 */
public record BaselineSpec(long recordCount, int customerCount, int segmentsPerCustomer, long userPoolSize,
                           double segmentSizeSkew, Long seed) {

    public BaselineSpec {
        if (recordCount <= 0 || customerCount <= 0 || segmentsPerCustomer <= 0 || userPoolSize <= 0) {
            throw new IllegalArgumentException("counts and sizes must be positive");
        }
        if (!(segmentSizeSkew >= 0 && segmentSizeSkew <= 4)) {
            throw new IllegalArgumentException("segment size skew must be between 0 and 4 (got " + segmentSizeSkew + ")");
        }
        if ((long) customerCount * segmentsPerCustomer > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many segments: " + (long) customerCount * segmentsPerCustomer);
        }
        // Each (segment, person) pair appears at most once, so a segment holds at most userPoolSize rows.
        long segments = (long) customerCount * segmentsPerCustomer;
        if (Math.multiplyHigh(segments, userPoolSize) == 0 && recordCount > segments * userPoolSize) {
            throw new IllegalArgumentException("recordCount " + recordCount + " exceeds the " + segments * userPoolSize
                    + " unique (segment, user) pairs available; raise user-pool-size or the segment count");
        }
    }

    int segmentCount() {
        return customerCount * segmentsPerCustomer;
    }
}
