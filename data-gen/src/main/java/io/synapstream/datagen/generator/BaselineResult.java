package io.synapstream.datagen.generator;

/**
 * @param rows       rows written
 * @param population segment state after the baseline, to save for the first mutation
 */
public record BaselineResult(long rows, SegmentPopulation population) {
}
