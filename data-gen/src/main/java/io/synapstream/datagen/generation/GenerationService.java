package io.synapstream.datagen.generation;

import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generation.RunStatus.Mode;
import io.synapstream.datagen.generation.RunStatus.State;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import io.synapstream.datagen.generator.AudienceGenerator;
import io.synapstream.datagen.generator.BaselineSpec;
import io.synapstream.datagen.generator.GenerationMonitor;
import io.synapstream.datagen.generator.MutationResult;
import io.synapstream.datagen.generator.MutationSpec;
import io.synapstream.datagen.generator.WriterSettings;
import io.synapstream.datagen.snapshot.SnapshotInfo;
import io.synapstream.datagen.snapshot.SnapshotInfo.Kind;
import io.synapstream.datagen.snapshot.SnapshotStore;
import io.synapstream.datagen.snapshot.SnapshotStore.Manifest;
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
 * <p>
 * A run mutates the latest snapshot when one exists, and otherwise (or when forced) generates a full baseline.
 */
@Service
public class GenerationService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(GenerationService.class);

    private static final DateTimeFormatter FILE_TIMESTAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    /** Expected-delta files live in a subdirectory so a glob over the snapshot directory never picks them up. */
    static final String DELTA_DIR = "expected-deltas";

    private final DataGenProperties properties;
    private final AudienceGenerator generator;
    private final SnapshotStore snapshots;
    private final ExecutorService executor =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("datagen-run").factory());

    private final AtomicReference<ActiveRun> active = new AtomicReference<>();
    private volatile RunStatus lastRun;

    public GenerationService(DataGenProperties properties, AudienceGenerator generator, SnapshotStore snapshots) {
        this.properties = properties;
        this.generator = generator;
        this.snapshots = snapshots;
    }

    /**
     * Starts a run in the background.
     *
     * @param forceFull   generate a fresh baseline even if a previous snapshot exists
     * @param recordCount rows for a full run, or {@code null} for the configured default; only allowed with
     *                    {@code forceFull}
     * @throws GenerationAlreadyRunningException if a run is in progress
     * @throws IllegalArgumentException          if {@code recordCount} is given without {@code forceFull}, or a
     *                                           baseline would need more unique (segment, user) pairs than exist
     */
    public RunStatus start(Trigger trigger, boolean forceFull, Long recordCount) {
        if (recordCount != null && !forceFull) {
            throw new IllegalArgumentException("records can only be set for a full run (mode=full)");
        }
        Instant now = Instant.now();
        String id = UUID.randomUUID().toString().substring(0, 8);
        ActiveRun run = new ActiveRun(id, trigger, now);
        if (!active.compareAndSet(null, run)) {
            ActiveRun current = active.get();
            // The other run may have finished between the CAS and get(); report this one's id if so.
            throw new GenerationAlreadyRunningException((current != null ? current : run).status());
        }
        try {
            // Planned after claiming the slot, so the previous run's manifest commit is guaranteed to be visible.
            Manifest manifest = snapshots.load();
            SnapshotInfo base = forceFull ? null : snapshots.latestUsable(manifest).orElse(null);
            long sequence = manifest.latest().map(latest -> latest.sequence() + 1).orElse(0L);
            Long seed = properties.seed() != null ? properties.seed() + sequence : null;
            // Built here rather than in the background so an impossible size is rejected immediately.
            BaselineSpec baseline = base != null ? null : new BaselineSpec(
                    recordCount != null ? recordCount : properties.recordCount(), properties.customerCount(),
                    properties.segmentsPerCustomer(), properties.userPoolSize(), seed);
            String name = "audiences-" + FILE_TIMESTAMP.format(now) + "-" + id;
            run.plan(
                    base,
                    sequence,
                    seed,
                    manifest.nextUserId(),
                    baseline,
                    snapshots.resolve(name + ".parquet"),
                    base == null ? null : snapshots.resolve(DELTA_DIR + "/" + name + ".parquet"));
        } catch (RuntimeException e) {
            active.set(null);
            throw e;
        }
        log.info("Starting {} {} generation run {} -> {}", trigger, run.mode, id, run.file);
        executor.execute(() -> execute(run));
        return run.status();
    }

    /**
     * Requests the active run to stop. The run aborts within a few thousand rows and its partial files are deleted.
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
            WriterSettings writer = new WriterSettings(properties.compression(), properties.rowGroupSize().toBytes());
            SnapshotInfo snapshot;
            long nextUserId;
            if (run.mode == Mode.FULL) {
                long rows = generator.generateBaseline(run.baseline, writer, run.file, run);
                snapshot = new SnapshotInfo(run.id, run.sequence, Kind.FULL, null, Instant.now(),
                        snapshots.relativize(run.file), null, rows, null, null, null);
                nextUserId = properties.userPoolSize();
            } else {
                MutationSpec spec = new MutationSpec(properties.customerCount(), properties.segmentsPerCustomer(),
                        properties.mutation().minRate(), properties.mutation().maxRate(), run.firstNewUserId, run.seed);
                MutationResult result = generator.mutate(spec, writer, snapshots.resolve(run.base.file()),
                        run.file, run.deltaFile, run);
                snapshot = new SnapshotInfo(run.id, run.sequence, Kind.MUTATION, run.base.id(), Instant.now(),
                        snapshots.relativize(run.file), snapshots.relativize(run.deltaFile), result.rows(),
                        result.added(), result.removed(), result.mutationRate());
                nextUserId = run.firstNewUserId + result.added();
                run.added = result.added();
                run.removed = result.removed();
            }
            snapshots.commit(snapshot, nextUserId);
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
        log.info("Generation run {} {} after {}: {} rows processed, {} added, {} removed", run.id, outcome,
                Duration.between(run.startedAt, finished), run.processed, run.added, run.removed);
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

        // Set by plan() before the run is handed to the executor.
        volatile Mode mode;
        volatile SnapshotInfo base;
        volatile long sequence;
        volatile Long seed;
        volatile long firstNewUserId;
        volatile BaselineSpec baseline;
        volatile Path file;
        volatile Path deltaFile;

        volatile long target;
        volatile long processed;
        volatile Long added;
        volatile Long removed;
        volatile boolean cancelRequested;

        ActiveRun(String id, Trigger trigger, Instant startedAt) {
            this.id = id;
            this.trigger = trigger;
            this.startedAt = startedAt;
        }

        void plan(SnapshotInfo base, long sequence, Long seed, long firstNewUserId, BaselineSpec baseline,
                  Path file, Path deltaFile) {
            this.base = base;
            this.mode = base == null ? Mode.FULL : Mode.MUTATION;
            this.sequence = sequence;
            this.seed = seed;
            this.firstNewUserId = firstNewUserId;
            this.baseline = baseline;
            this.file = file;
            this.deltaFile = deltaFile;
        }

        @Override
        public void target(long totalRows) {
            this.target = totalRows;
        }

        @Override
        public void progress(long rowsProcessed) {
            this.processed = rowsProcessed;
        }

        @Override
        public boolean isCancelled() {
            return cancelRequested;
        }

        RunStatus status() {
            return finish(cancelRequested ? State.STOPPING : State.RUNNING, null, null);
        }

        RunStatus finish(State state, Instant finishedAt, String error) {
            SnapshotInfo b = base;
            Path f = file;
            Path d = deltaFile;
            return new RunStatus(id, trigger, mode, state, startedAt, finishedAt, b != null ? b.id() : null,
                    target, processed, added, removed, f != null ? f.toString() : null,
                    d != null ? d.toString() : null, error);
        }
    }
}
