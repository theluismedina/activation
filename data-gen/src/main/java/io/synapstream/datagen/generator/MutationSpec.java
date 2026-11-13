package io.synapstream.datagen.generator;

import java.util.Objects;

/**
 * Derives a new snapshot from the previous one by removing some rows and adding new ones.
 *
 * @param minRate    lower bound of the fraction of the previous snapshot's rows that change
 * @param maxRate    upper bound of that fraction; the actual rate is drawn uniformly per run
 * @param population the previous snapshot's segments and where each one's walk through the pool stands
 * @param seed       RNG seed, or {@code null} for a random one
 */
public record MutationSpec(double minRate, double maxRate, SegmentPopulation population, Long seed) {

    public MutationSpec {
        if (!(minRate > 0 && minRate <= maxRate && maxRate < 1)) {
            throw new IllegalArgumentException("require 0 < minRate <= maxRate < 1");
        }
        Objects.requireNonNull(population, "population");
    }
}
