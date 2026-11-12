package io.synapstream.datagen.generator;

/**
 * Lets the caller observe progress and ask a running generation to stop.
 */
public interface GenerationMonitor {

    /** Called once the total number of rows the run will process is known. */
    default void target(long totalRows) {
    }

    /** Called periodically with the number of rows processed so far. */
    void progress(long rowsProcessed);

    /** Polled periodically; returning {@code true} aborts the run and discards its partial files. */
    boolean isCancelled();

    GenerationMonitor NONE = new GenerationMonitor() {
        @Override
        public void progress(long rowsProcessed) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };
}
