package io.synapstream.datagen.generation;

import java.time.Instant;

/**
 * Snapshot of a generation run, either in progress or finished.
 *
 * @param finishedAt null while the run is active
 * @param file       the final output path; only exists on disk once {@code state} is COMPLETED
 * @param error      failure message when {@code state} is FAILED
 */
public record RunStatus(
        String id,
        Trigger trigger,
        State state,
        Instant startedAt,
        Instant finishedAt,
        long targetRecords,
        long recordsWritten,
        String file,
        String error) {

    public enum Trigger { MANUAL, SCHEDULED }

    public enum State { RUNNING, STOPPING, COMPLETED, CANCELLED, FAILED }
}
