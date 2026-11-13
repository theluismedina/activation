package io.synapstream.datagen.generator;

import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * Which people each segment can still add, saved in the manifest so mutations continue where the last run stopped.
 * <p>
 * Every segment draws its members from one shared pool of people, ids {@code [0, poolSize)}, so the same person
 * belongs to many segments. Within a segment, members never repeat: segment {@code s} walks through the pool from a
 * random start in steps of {@code steps[s]}, which is coprime with the pool size, so the walk visits every person once
 * before repeating. {@code next[s]} is the next person on the walk and {@code used[s]} how many it has handed out.
 * Nobody is added to a segment twice, not even after being removed from it.
 * <p>
 * Segment ids are globally unique: segment index {@code s} has id {@code s + 1} and belongs to customer
 * {@code s / segmentsPerCustomer + 1}.
 */
public record SegmentPopulation(int segmentsPerCustomer, long poolSize, long[] steps, long[] next, long[] used) {

    public SegmentPopulation {
        if (segmentsPerCustomer <= 0 || poolSize <= 0) {
            throw new IllegalArgumentException("segmentsPerCustomer and poolSize must be positive");
        }
        if (steps.length == 0 || steps.length != next.length || steps.length != used.length) {
            throw new IllegalArgumentException("steps, next and used must have one entry per segment");
        }
    }

    /** New walks starting at random positions in the pool, with nobody handed out yet. */
    static SegmentPopulation create(int segments, int segmentsPerCustomer, long poolSize, SplittableRandom random) {
        long[] steps = new long[segments];
        long[] next = new long[segments];
        for (int s = 0; s < segments; s++) {
            next[s] = random.nextLong(poolSize);
            steps[s] = coprimeStep(poolSize, random);
        }
        return new SegmentPopulation(segmentsPerCustomer, poolSize, steps, next, new long[segments]);
    }

    public int segmentCount() {
        return steps.length;
    }

    long customerId(int segment) {
        return segment / segmentsPerCustomer + 1L;
    }

    /** A working copy that hands out members; {@link Walker#population()} gives the state afterwards. */
    Walker walker() {
        return new Walker(next.clone(), used.clone());
    }

    final class Walker {
        private final long[] next;
        private final long[] used;

        private Walker(long[] next, long[] used) {
            this.next = next;
            this.used = used;
        }

        long customerId(int segment) {
            return SegmentPopulation.this.customerId(segment);
        }

        boolean exhausted(int segment) {
            return used[segment] >= poolSize;
        }

        /** The next person for {@code segment}; never one it has had before. */
        long next(int segment) {
            if (exhausted(segment)) {
                throw new IllegalStateException("Segment " + (segment + 1) + " has used every person in the pool");
            }
            long person = next[segment];
            // Both terms are below poolSize, so the sum can't overflow.
            next[segment] = (person + steps[segment]) % poolSize;
            used[segment]++;
            return person;
        }

        SegmentPopulation population() {
            return new SegmentPopulation(segmentsPerCustomer, poolSize, steps, next.clone(), used.clone());
        }
    }

    private static long coprimeStep(long poolSize, SplittableRandom random) {
        if (poolSize == 1) {
            return 1;
        }
        long candidate;
        do {
            candidate = random.nextLong(1, poolSize);
        } while (gcd(candidate, poolSize) != 1);
        return candidate;
    }

    private static long gcd(long a, long b) {
        while (b != 0) {
            long t = a % b;
            a = b;
            b = t;
        }
        return a;
    }

    // The generated equals/hashCode would compare the arrays by identity; compare their contents instead.
    @Override
    public boolean equals(Object o) {
        return o instanceof SegmentPopulation p && segmentsPerCustomer == p.segmentsPerCustomer
                && poolSize == p.poolSize && Arrays.equals(steps, p.steps) && Arrays.equals(next, p.next)
                && Arrays.equals(used, p.used);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Integer.hashCode(segmentsPerCustomer) + Long.hashCode(poolSize)) + Arrays.hashCode(used);
    }

    @Override
    public String toString() {
        return "SegmentPopulation[segments=" + segmentCount() + ", poolSize=" + poolSize + "]";
    }
}
