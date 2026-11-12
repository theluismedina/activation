package io.synapstream.datagen.generation;

import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generation.RunStatus.State;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import io.synapstream.datagen.generator.AudienceGenerator;
import io.synapstream.datagen.generator.GenerationMonitor;
import io.synapstream.datagen.generator.GenerationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs audience generation in the background, at most one run at a time, and supports stopping a run.
 */
@Service
public class GenerationService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(GenerationService.class);

    private static final DateTimeFormatter FILE_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private final DataGenProperties properties;
    private final AudienceGenerator generator;
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("datagen-run").factory());

    private final AtomicReference<ActiveRun> active = new AtomicReference<>();
    private volatile RunStatus lastRun;

    public GenerationService(DataGenProperties properties, AudienceGenerator generator) {
        this.properties = properties;
        this.generator = generator;
    }

    /**
     * Starts a run in the background.
     *
     * @param recordCount rows to write, or {@code null} for the configured default
     * @throws GenerationAlreadyRunningException if a run is in progress
     */
    public RunStatus start(Trigger trigger, Long recordCount) {
        long count = recordCount != null ? recordCount : properties.recordCount();
        Instant now = Instant.now();
        String id = UUID.randomUUID().toString().substring(0, 8);
        Path file = properties.outputDir()
                .resolve("audiences-" + FILE_TIMESTAMP.format(now) + "-" + id + ".parquet")
                .toAbsolutePath();
        GenerationSpec spec = new GenerationSpec(
                count,
                properties.customerCount(),
                properties.segmentsPerCustomer(),
                properties.userPoolSize(),
                properties.seed(),
                properties.compression(),
                properties.rowGroupSize().toBytes());

        ActiveRun run = new ActiveRun(id, trigger, now, spec, file);
        if (!active.compareAndSet(null, run)) {
            ActiveRun current = active.get();
            // The other run may have finished between the CAS and get(); report this one's id if so.
            throw new GenerationAlreadyRunningException(current != null ? current.status() : run.status());
        }
        log.info("Starting {} generation run {}: {} records -> {}", trigger, id, count, file);
        executor.execute(() -> execute(run));
        return run.status();
    }

    /**
     * Requests the active run to stop. The run aborts within a few thousand rows and its partial file is deleted.
     *
     * @return the run being stopped, or empty if nothing is running
     */
    public Optional<RunStatus> stop() {
        ActiveRun run = active.get();
        if (run == null) {
            return Optional.empty();
        }
        run.cancelRequested = true;
        log.info("Stop requested for generation run {}", run.id);
        return Optional.of(run.status());
    }

    public Optional<RunStatus> current() {
        return Optional.ofNullable(active.get()).map(ActiveRun::status);
    }

    public Optional<RunStatus> lastRun() {
        return Optional.ofNullable(lastRun);
    }

    private void execute(ActiveRun run) {
        State outcome;
        String error = null;
        try {
            generator.generate(run.spec, run.file, run);
            outcome = State.COMPLETED;
        } catch (CancellationException e) {
            outcome = State.CANCELLED;
        } catch (Exception | OutOfMemoryError e) {
            outcome = State.FAILED;
            error = e.toString();
            log.error("Generation run {} failed", run.id, e);
        }
        Instant finished = Instant.now();
        // Publish the result before clearing the active run so status never shows neither.
        lastRun = run.finish(outcome, finished, error);
        active.set(null);
        log.info("Generation run {} {} after {}: {} records", run.id, outcome,
                Duration.between(run.startedAt, finished), run.rowsWritten);
    }

    @Override
    public void destroy() throws InterruptedException {
        stop();
        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }
    }

    private static final class ActiveRun implements GenerationMonitor {
        final String id;
        final Trigger trigger;
        final Instant startedAt;
        final GenerationSpec spec;
        final Path file;
        volatile long rowsWritten;
        volatile boolean cancelRequested;

        ActiveRun(String id, Trigger trigger, Instant startedAt, GenerationSpec spec, Path file) {
            this.id = id;
            this.trigger = trigger;
            this.startedAt = startedAt;
            this.spec = spec;
            this.file = file;
        }

        @Override
        public void progress(long rowsWritten) {
            this.rowsWritten = rowsWritten;
        }

        @Override
        public boolean isCancelled() {
            return cancelRequested;
        }

        RunStatus status() {
            return finish(cancelRequested ? State.STOPPING : State.RUNNING, null, null);
        }

        RunStatus finish(State state, Instant finishedAt, String error) {
            return new RunStatus(id, trigger, state, startedAt, finishedAt, spec.recordCount(), rowsWritten,
                    file.toString(), error);
        }
    }
}
