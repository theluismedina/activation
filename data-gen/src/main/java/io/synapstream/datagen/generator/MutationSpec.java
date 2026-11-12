package io.synapstream.datagen.generator;

/**
 * Derives a new snapshot from the previous one by removing some rows and adding new ones.
 *
 * @param minRate         lower bound of the fraction of the previous snapshot's rows that change
 * @param maxRate         upper bound of that fraction; the actual rate is drawn uniformly per run
 * @param firstNewUserId  first never-used user id; added rows use ids from here up, so every add is new
 * @param seed            RNG seed, or {@code null} for a random one
 */
public record MutationSpec(int customerCount, int segmentsPerCustomer, double minRate, double maxRate,
                           long firstNewUserId, Long seed) {

    public MutationSpec {
        if (customerCount <= 0 || segmentsPerCustomer <= 0) {
            throw new IllegalArgumentException("counts must be positive");
        }
        if (!(minRate > 0 && minRate <= maxRate && maxRate < 1)) {
            throw new IllegalArgumentException("require 0 < minRate <= maxRate < 1");
        }
    }

    int segmentCount() {
        return customerCount * segmentsPerCustomer;
    }
}
