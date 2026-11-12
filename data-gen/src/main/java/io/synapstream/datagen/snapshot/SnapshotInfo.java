package io.synapstream.datagen.snapshot;

import java.time.Instant;

/**
 * One snapshot recorded in the manifest.
 *
 * @param sequence     increases by one with every snapshot, including fresh baselines
 * @param parentId     the snapshot this one was mutated from; null for a FULL snapshot
 * @param file         snapshot file, relative to the output directory
 * @param deltaFile    expected-delta file (exact rows added and removed relative to the parent), relative to the
 *                     output directory; null for a FULL snapshot
 * @param added        rows added relative to the parent; null for a FULL snapshot
 * @param removed      rows removed relative to the parent; null for a FULL snapshot
 * @param mutationRate rate drawn for this run; null for a FULL snapshot
 */
public record SnapshotInfo(
        String id,
        long sequence,
        Kind kind,
        String parentId,
        Instant createdAt,
        String file,
        String deltaFile,
        long rows,
        Long added,
        Long removed,
        Double mutationRate) {

    public enum Kind { FULL, MUTATION }
}
