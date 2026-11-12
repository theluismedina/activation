package io.synapstream.datagen.generator;

/**
 * @param rows         rows in the new snapshot
 * @param mutationRate (added + removed) / rows in the previous snapshot, as drawn for this run
 */
public record MutationResult(long rows, long added, long removed, double mutationRate) {
}
