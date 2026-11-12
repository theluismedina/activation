package io.synapstream.datagen.generator;

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SplittableRandom;
import java.util.concurrent.CancellationException;

/**
 * Writes a Parquet file of random audience rows: {@code (customer_id, segment_id, hashed_user_id)}.
 * <p>
 * Each row picks a random customer, one of that customer's segments, and a user from a fixed pool. The user is
 * hashed the way ad platforms expect identifiers: lowercase hex SHA-256 of a normalized email address.
 * <p>
 * The file is written under a temporary name and moved into place only once complete, so readers never see a
 * partial file.
 */
@Component
public class AudienceGenerator {

    /** How often (in rows) progress is reported and cancellation is checked. */
    static final int CHECK_INTERVAL = 50_000;

    static final String IN_PROGRESS_SUFFIX = ".inprogress";

    private static final byte[] HEX = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    /**
     * @return the number of rows written (always {@code spec.recordCount()} on success)
     * @throws CancellationException if the monitor requested cancellation; no output file is left behind
     */
    public long generate(GenerationSpec spec, Path target, GenerationMonitor monitor) throws IOException {
        Path parent = target.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        Path temp = parent.resolve(target.getFileName() + IN_PROGRESS_SUFFIX);

        boolean success = false;
        try {
            long written = writeRows(spec, temp, monitor);
            moveIntoPlace(temp, target);
            success = true;
            return written;
        } finally {
            if (!success) {
                Files.deleteIfExists(temp);
            }
        }
    }

    private long writeRows(GenerationSpec spec, Path file, GenerationMonitor monitor) throws IOException {
        SplittableRandom random = spec.seed() != null ? new SplittableRandom(spec.seed()) : new SplittableRandom();
        MessageDigest sha256 = sha256();

        try (ParquetWriter<Audience> writer = new AudienceWriteSupport.Builder(new LocalOutputFile(file))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(spec.compression())
                .withRowGroupSize(spec.rowGroupSizeBytes())
                // Hashes are near-unique, so a dictionary only costs CPU before Parquet gives up on it.
                .withDictionaryEncoding(AudienceWriteSupport.HASHED_USER_ID, false)
                .build()) {

            for (long row = 0; row < spec.recordCount(); row++) {
                if (row % CHECK_INTERVAL == 0) {
                    if (monitor.isCancelled()) {
                        throw new CancellationException("Cancelled after " + row + " rows");
                    }
                    monitor.progress(row);
                }
                writer.write(nextAudience(spec, random, sha256));
            }
        }
        monitor.progress(spec.recordCount());
        return spec.recordCount();
    }

    private static Audience nextAudience(GenerationSpec spec, SplittableRandom random, MessageDigest sha256) {
        long customerId = random.nextInt(spec.customerCount()) + 1L;
        // Segment ids are globally unique: customer N owns ids ((N-1)*S, N*S].
        long segmentId = (customerId - 1) * spec.segmentsPerCustomer() + random.nextInt(spec.segmentsPerCustomer()) + 1;
        long userId = random.nextLong(spec.userPoolSize());
        return new Audience(customerId, segmentId, hashedEmail(userId, sha256));
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

    private static void moveIntoPlace(Path temp, Path target) throws IOException {
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JVM", e);
        }
    }
}
