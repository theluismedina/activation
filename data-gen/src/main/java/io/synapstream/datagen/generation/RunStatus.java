package io.synapstream.datagen.generation;

import java.time.Instant;

/**
 * Snapshot of a generation run, either in progress or finished.
 *
 * @param mode             FULL builds a baseline from scratch; MUTATION derives a snapshot from {@code baseSnapshot}
 * @param baseSnapshot     id of the snapshot being mutated; null for FULL runs
 * @param targetRecords    rows the run will process (for a mutation: previous rows + adds); 0 until known
 * @param recordsProcessed rows processed so far
 * @param added            rows added (MUTATION runs, once completed)
 * @param removed          rows removed (MUTATION runs, once completed)
 * @param file             the snapshot file; only exists on disk once {@code state} is COMPLETED
 * @param deltaFile        the expected-delta file for MUTATION runs
 * @param error            failure message when {@code state} is FAILED
 */
public record RunStatus(
        String id,
        Trigger trigger,
        Mode mode,
        State state,
        Instant startedAt,
        Instant finishedAt,
        String baseSnapshot,
        long targetRecords,
        long recordsProcessed,
        Long added,
        Long removed,
        String file,
        String deltaFile,
        String error) {

    public enum Trigger { MANUAL, SCHEDULED }

    public enum Mode { FULL, MUTATION }

    public enum State { RUNNING, STOPPING, COMPLETED, CANCELLED, FAILED }
}
