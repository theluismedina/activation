package io.synapstream.datagen.generator;

/**
 * @param rows         rows in the new snapshot
 * @param mutationRate (added + removed) / rows in the previous snapshot, as drawn for this run
 * @param population   segment state after this run, to save for the next one
 */
public record MutationResult(long rows, long added, long removed, double mutationRate, SegmentPopulation population) {
}
