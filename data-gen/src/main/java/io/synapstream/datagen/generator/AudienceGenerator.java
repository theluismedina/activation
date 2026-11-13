package io.synapstream.datagen.generator;

import io.synapstream.datagen.generator.AudienceWriteSupport.DeltaRow;
import io.synapstream.datagen.generator.AudienceWriteSupport.Op;
import io.synapstream.datagen.support.AtomicFiles;
import org.apache.parquet.hadoop.ParquetWriter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;

/**
 * Produces audience snapshots: Parquet files of {@code (customer_id, segment_id, hashed_user_id)}.
 * <ul>
 *   <li>{@link #generateBaseline} creates a snapshot from scratch.</li>
 *   <li>{@link #mutate} derives a new snapshot from the previous one by removing and adding a small fraction of rows,
 *       and also writes the exact changes to an expected-delta file that a delta job's output can be checked
 *       against.</li>
 * </ul>
 * A snapshot never contains the same (segment, user) pair twice, so every change is unambiguous. Users come from
 * one pool shared by all segments, so each person belongs to several segments, across customers too; segment sizes
 * have a long tail (see {@link BaselineSpec#segmentSizeSkew()}), and mutations keep that shape by adding to each
 * segment in proportion to its current size. {@link SegmentPopulation} describes the segments and is carried from
 * run to run.
 * <p>
 * User ids are hashed the way ad platforms expect: lowercase hex SHA-256 of a normalized email address.
 * <p>
 * Files are written under a temporary name and moved into place only once complete.
 */
@Component
public class AudienceGenerator {

    /** How often (in rows) progress is reported and cancellation is checked. */
    static final int CHECK_INTERVAL = 50_000;

    public static final String IN_PROGRESS_SUFFIX = ".inprogress";

    private static final byte[] HEX = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    /**
     * @return the number of rows written, and the segment state to carry into the first mutation
     * @throws CancellationException if the monitor requested cancellation; no output file is left behind
     */
    public BaselineResult generateBaseline(BaselineSpec spec, WriterSettings settings, Path target, GenerationMonitor monitor)
            throws IOException {
        monitor.target(spec.recordCount());
        Path temp = tempFor(target);
        boolean done = false;
        try {
            SplittableRandom random = random(spec.seed());
            SegmentPopulation.Walker users = SegmentPopulation.create(spec.segmentCount(), spec.segmentsPerCustomer(),
                    spec.userPoolSize(), random).walker();
            SegmentPicker segments = SegmentPicker.logNormal(spec.segmentCount(), spec.segmentSizeSkew(), random);
            MessageDigest sha256 = sha256();

            try (ParquetWriter<Audience> writer = AudienceWriteSupport.snapshotWriter(temp, settings)) {
                for (long row = 0; row < spec.recordCount(); row++) {
                    checkpoint(row, monitor);
                    int segment = segments.pick(random, s -> !users.exhausted(s));
                    writer.write(audience(users, segment, sha256));
                }
            }
            monitor.progress(spec.recordCount());
            AtomicFiles.moveIntoPlace(temp, target);
            done = true;
            return new BaselineResult(spec.recordCount(), users.population());
        } finally {
            if (!done) {
                Files.deleteIfExists(temp);
            }
        }
    }

    /**
     * Copies {@code previous} to {@code target}, removing a random set of rows and appending new ones. The total
     * number of changed rows is a rate drawn from {@code [minRate, maxRate]} times the previous row count; between a
     * quarter and three quarters of the changes are removes, so audiences drift up and down a little over time.
     * Removes are uniform over rows, and adds go to each segment in proportion to its size, so segment sizes keep
     * their shape. Each add is a person the segment has never had, usually one already in other segments.
     * <p>
     * Every removed and added row is also written to {@code deltaTarget} with an {@code op} column.
     *
     * @throws CancellationException if the monitor requested cancellation; no output files are left behind
     */
    public MutationResult mutate(MutationSpec spec, WriterSettings settings, Path previous, Path target,
                                 Path deltaTarget, GenerationMonitor monitor) throws IOException {
        Path temp = tempFor(target);
        Path deltaTemp = tempFor(deltaTarget);
        boolean done = false;
        try (AudienceReader reader = AudienceReader.open(previous)) {
            SplittableRandom random = random(spec.seed());
            long previousRows = reader.rowCount();
            double rate = spec.minRate() + random.nextDouble() * (spec.maxRate() - spec.minRate());
            long changes = Math.max(1, Math.round(previousRows * rate));
            long removes = Math.min(previousRows, Math.round(changes * (0.25 + random.nextDouble() * 0.5)));
            long adds = changes - removes;
            monitor.target(previousRows + adds);

            SegmentPopulation population = spec.population();
            long[] sizes = new long[population.segmentCount()];
            MessageDigest sha256 = sha256();
            try (ParquetWriter<Audience> snapshot = AudienceWriteSupport.snapshotWriter(temp, settings);
                 ParquetWriter<DeltaRow> delta = AudienceWriteSupport.deltaWriter(deltaTemp, settings)) {

                // Selection sampling (Knuth's Algorithm S): removes exactly `removes` rows, uniformly at random,
                // in a single pass with no extra memory.
                long seen = 0;
                long removed = 0;
                Audience row;
                while ((row = reader.read()) != null) {
                    checkpoint(seen, monitor);
                    if (random.nextLong(previousRows - seen) < removes - removed) {
                        delta.write(new DeltaRow(row, Op.REMOVE));
                        removed++;
                    } else {
                        snapshot.write(row);
                        int segment = Math.toIntExact(row.segmentId() - 1);
                        if (segment < 0 || segment >= sizes.length) {
                            throw new IOException("Segment " + row.segmentId() + " in " + previous + " is not one of "
                                    + "the " + sizes.length + " segments in the manifest; run a full baseline");
                        }
                        sizes[segment]++;
                    }
                    seen++;
                }
                if (seen != previousRows) {
                    throw new IOException("Expected " + previousRows + " rows in " + previous + " but read " + seen);
                }

                SegmentPopulation.Walker users = population.walker();
                SegmentPicker segments = SegmentPicker.proportional(sizes);
                for (long i = 0; i < adds; i++) {
                    checkpoint(previousRows + i, monitor);
                    int segment = segments.pick(random, s -> !users.exhausted(s));
                    Audience added = audience(users, segment, sha256);
                    snapshot.write(added);
                    delta.write(new DeltaRow(added, Op.ADD));
                }
                population = users.population();
            }
            monitor.progress(previousRows + adds);
            AtomicFiles.moveIntoPlace(deltaTemp, deltaTarget);
            AtomicFiles.moveIntoPlace(temp, target);
            done = true;
            return new MutationResult(previousRows - removes + adds, adds, removes, rate, population);
        } finally {
            if (!done) {
                Files.deleteIfExists(temp);
                Files.deleteIfExists(deltaTemp);
                Files.deleteIfExists(deltaTarget);
            }
        }
    }

    private static Audience audience(SegmentPopulation.Walker users, int segment, MessageDigest sha256) {
        return new Audience(users.customerId(segment), segment + 1L, hashedEmail(users.next(segment), sha256));
    }

    private static void checkpoint(long row, GenerationMonitor monitor) {
        if (row % CHECK_INTERVAL == 0) {
            if (monitor.isCancelled()) {
                throw new CancellationException("Cancelled after " + row + " rows");
            }
            monitor.progress(row);
        }
    }

    /** Lowercase hex SHA-256 of {@code user<id>@example.com}, as ASCII bytes. */
    static byte[] hashedEmail(long userId, MessageDigest sha256) {
        byte[] digest = sha256.digest(("user" + userId + "@example.com").getBytes(StandardCharsets.US_ASCII));
        byte[] hex = new byte[digest.length * 2];
        for (int i = 0; i < digest.length; i++) {
            hex[i * 2] = HEX[(digest[i] >> 4) & 0xF];
            hex[i * 2 + 1] = HEX[digest[i] & 0xF];
        }
        return hex;
    }

    private static Path tempFor(Path target) throws IOException {
        Path absolute = target.toAbsolutePath();
        Files.createDirectories(absolute.getParent());
        return absolute.resolveSibling(absolute.getFileName() + IN_PROGRESS_SUFFIX);
    }

    private static SplittableRandom random(Long seed) {
        return seed != null ? new SplittableRandom(seed) : new SplittableRandom();
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }
}
