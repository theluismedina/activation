package io.synapstream.datagen.generator;

/**
 * A full snapshot generated from scratch.
 *
 * @param userPoolSize users are drawn from ids {@code [0, userPoolSize)}
 * @param seed         RNG seed, or {@code null} for a random one
 */
public record BaselineSpec(long recordCount, int customerCount, int segmentsPerCustomer, long userPoolSize, Long seed) {

    public BaselineSpec {
        if (recordCount <= 0 || customerCount <= 0 || segmentsPerCustomer <= 0 || userPoolSize <= 0) {
            throw new IllegalArgumentException("counts and sizes must be positive");
        }
        // Each (segment, user) pair appears at most once, so a segment holds at most userPoolSize rows.
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
