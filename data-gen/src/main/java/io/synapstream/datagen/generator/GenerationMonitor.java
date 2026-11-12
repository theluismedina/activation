package io.synapstream.datagen.generator;

/**
 * Lets the caller observe progress and ask a running generation to stop.
 */
public interface GenerationMonitor {

    /** Called periodically with the total number of rows written so far. */
    void progress(long rowsWritten);

    /** Polled periodically; returning {@code true} aborts the run and discards the partial file. */
    boolean isCancelled();

    GenerationMonitor NONE = new GenerationMonitor() {
        @Override
        public void progress(long rowsWritten) {
        }

        @Override
        public boolean isCancelled() {
            return false;
        }
    };
}
