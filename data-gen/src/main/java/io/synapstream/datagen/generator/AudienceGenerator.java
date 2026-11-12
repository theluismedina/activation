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
 * A snapshot never contains the same (segment, user) pair twice, so every change is unambiguous.
 * <p>
 * Segment ids are globally unique: segment {@code s+1} (for {@code s} in {@code [0, customers * segmentsPerCustomer)})
 * belongs to customer {@code s / segmentsPerCustomer + 1}. User ids are hashed the way ad platforms expect: lowercase
 * hex SHA-256 of a normalized email address.
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
     * @return the number of rows written
     * @throws CancellationException if the monitor requested cancellation; no output file is left behind
     */
    public long generateBaseline(BaselineSpec spec, WriterSettings settings, Path target, GenerationMonitor monitor)
            throws IOException {
        monitor.target(spec.recordCount());
        Path temp = tempFor(target);
        boolean done = false;
        try {
            SplittableRandom random = random(spec.seed());
            UniqueUserPicker users = new UniqueUserPicker(spec, random);
            MessageDigest sha256 = sha256();

            try (ParquetWriter<Audience> writer = AudienceWriteSupport.snapshotWriter(temp, settings)) {
                for (long row = 0; row < spec.recordCount(); row++) {
                    checkpoint(row, monitor);
                    int segment = users.nextSegment(random);
                    writer.write(audience(segment, spec.segmentsPerCustomer(), users.nextUser(segment), sha256));
                }
            }
            monitor.progress(spec.recordCount());
            AtomicFiles.moveIntoPlace(temp, target);
            done = true;
            return spec.recordCount();
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
                    }
                    seen++;
                }
                if (seen != previousRows) {
                    throw new IOException("Expected " + previousRows + " rows in " + previous + " but read " + seen);
                }

                long userId = spec.firstNewUserId();
                for (long i = 0; i < adds; i++) {
                    checkpoint(previousRows + i, monitor);
                    int segment = random.nextInt(spec.segmentCount());
                    Audience added = audience(segment, spec.segmentsPerCustomer(), userId++, sha256);
                    snapshot.write(added);
                    delta.write(new DeltaRow(added, Op.ADD));
                }
            }
            monitor.progress(previousRows + adds);
            AtomicFiles.moveIntoPlace(deltaTemp, deltaTarget);
            AtomicFiles.moveIntoPlace(temp, target);
            done = true;
            return new MutationResult(previousRows - removes + adds, adds, removes, rate);
        } finally {
            if (!done) {
                Files.deleteIfExists(temp);
                Files.deleteIfExists(deltaTemp);
                Files.deleteIfExists(deltaTarget);
            }
        }
    }

    private static Audience audience(int segment, int segmentsPerCustomer, long userId, MessageDigest sha256) {
        return new Audience(segment / segmentsPerCustomer + 1L, segment + 1L, hashedEmail(userId, sha256));
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

    /**
     * Gives each segment a distinct sequence of users without tracking which users were used: segment {@code s}
     * walks {@code offset_s, offset_s + step_s, offset_s + 2*step_s, ...} modulo the pool size. Because each step is
     * coprime with the pool size, the walk visits every user once before repeating.
     */
    private static final class UniqueUserPicker {
        private final long poolSize;
        private final long[] next;
        private final long[] step;
        private final long[] used;

        UniqueUserPicker(BaselineSpec spec, SplittableRandom random) {
            this.poolSize = spec.userPoolSize();
            int segments = spec.segmentCount();
            this.next = new long[segments];
            this.step = new long[segments];
            this.used = new long[segments];
            for (int s = 0; s < segments; s++) {
                next[s] = random.nextLong(poolSize);
                step[s] = coprimeStep(random);
            }
        }

        /** A random segment that still has unused users. */
        int nextSegment(SplittableRandom random) {
            int segment;
            do {
                segment = random.nextInt(next.length);
            } while (used[segment] >= poolSize);
            return segment;
        }

        long nextUser(int segment) {
            long user = next[segment];
            next[segment] = (user + step[segment]) % poolSize;
            used[segment]++;
            return user;
        }

        private long coprimeStep(SplittableRandom random) {
            if (poolSize == 1) {
                return 1;
            }
            long candidate;
            do {
                candidate = random.nextLong(1, poolSize);
            } while (gcd(candidate, poolSize) != 1);
            return candidate;
        }

        private static long gcd(long a, long b) {
            while (b != 0) {
                long t = a % b;
                a = b;
                b = t;
            }
            return a;
        }
    }
}
