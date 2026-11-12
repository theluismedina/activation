package io.synapstream.datagen.generator;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AudienceGeneratorTest {

    private static final WriterSettings SETTINGS = new WriterSettings(CompressionCodecName.SNAPPY, 8L * 1024 * 1024);

    private final AudienceGenerator generator = new AudienceGenerator();

    @TempDir
    Path dir;

    @Test
    void baselineHasRequestedRowsValidRangesAndNoDuplicates() throws IOException {
        Path file = dir.resolve("out/baseline.parquet");

        long written = generator.generateBaseline(baseline(120_000, 42L), SETTINGS, file, GenerationMonitor.NONE);

        assertThat(written).isEqualTo(120_000);
        assertThat(file.resolveSibling(file.getFileName() + AudienceGenerator.IN_PROGRESS_SUFFIX)).doesNotExist();
        List<Group> rows = readAll(file, AudienceWriteSupport.SCHEMA);
        assertThat(rows).hasSize(120_000);
        for (Group row : rows) {
            long customer = row.getLong("customer_id", 0);
            long segment = row.getLong("segment_id", 0);
            // Each customer owns 5 segments: customer N -> ((N-1)*5, N*5]
            assertThat(customer).isBetween(1L, 10L);
            assertThat(segment).isBetween((customer - 1) * 5 + 1, customer * 5);
            assertThat(row.getString("hashed_user_id", 0)).hasSize(64).matches("[0-9a-f]+");
        }
        assertThat(keys(rows)).hasSize(120_000);
    }

    @Test
    void baselineCanFillEverySegmentWithoutDuplicates() throws IOException {
        // 50 segments x 100 users = exactly 5,000 unique pairs available
        Path file = dir.resolve("full.parquet");
        generator.generateBaseline(new BaselineSpec(5_000, 10, 5, 100, 1L), SETTINGS, file, GenerationMonitor.NONE);

        assertThat(keys(readAll(file, AudienceWriteSupport.SCHEMA))).hasSize(5_000);
    }

    @Test
    void baselineRejectsMoreRowsThanUniquePairs() {
        assertThatThrownBy(() -> new BaselineSpec(5_001, 10, 5, 100, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sameSeedProducesSameRows() throws IOException {
        Path a = dir.resolve("a.parquet");
        Path b = dir.resolve("b.parquet");
        generator.generateBaseline(baseline(1_000, 7L), SETTINGS, a, GenerationMonitor.NONE);
        generator.generateBaseline(baseline(1_000, 7L), SETTINGS, b, GenerationMonitor.NONE);

        assertThat(readAll(a, AudienceWriteSupport.SCHEMA).toString())
                .isEqualTo(readAll(b, AudienceWriteSupport.SCHEMA).toString());
    }

    @Test
    void mutationChangesBetweenOneAndFivePercentAndDeltaIsExact() throws IOException {
        Path previous = dir.resolve("s0.parquet");
        generator.generateBaseline(baseline(200_000, 3L), SETTINGS, previous, GenerationMonitor.NONE);

        Path next = dir.resolve("s1.parquet");
        Path delta = dir.resolve("deltas/s1.parquet");
        MutationResult result = generator.mutate(mutation(1_000_000, 4L), SETTINGS, previous, next, delta,
                GenerationMonitor.NONE);

        Set<String> before = keys(readAll(previous, AudienceWriteSupport.SCHEMA));
        List<Group> afterRows = readAll(next, AudienceWriteSupport.SCHEMA);
        Set<String> after = keys(afterRows);
        assertThat(after).as("no duplicates after mutation").hasSize(afterRows.size());

        Set<String> actuallyRemoved = new HashSet<>(before);
        actuallyRemoved.removeAll(after);
        Set<String> actuallyAdded = new HashSet<>(after);
        actuallyAdded.removeAll(before);

        Set<String> expectedRemoved = new HashSet<>();
        Set<String> expectedAdded = new HashSet<>();
        for (Group row : readAll(delta, AudienceWriteSupport.DELTA_SCHEMA)) {
            (row.getString("op", 0).equals("ADD") ? expectedAdded : expectedRemoved).add(key(row));
        }

        assertThat(actuallyRemoved).isEqualTo(expectedRemoved).hasSize((int) result.removed());
        assertThat(actuallyAdded).isEqualTo(expectedAdded).hasSize((int) result.added());
        assertThat(result.rows()).isEqualTo(after.size());

        double changed = (double) (result.added() + result.removed()) / before.size();
        assertThat(changed).isBetween(0.01, 0.05);
        assertThat(result.mutationRate()).isBetween(0.01, 0.05);
        assertThat(result.added()).isPositive();
        assertThat(result.removed()).isPositive();
    }

    @Test
    void hashIsHexSha256OfEmail() throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        String expected = HexFormat.of().formatHex(sha.digest("user123@example.com".getBytes()));

        assertThat(new String(AudienceGenerator.hashedEmail(123, sha))).isEqualTo(expected);
    }

    @Test
    void cancelledBaselineLeavesNoFileBehind() {
        Path file = dir.resolve("cancelled.parquet");

        assertThatThrownBy(() -> generator.generateBaseline(baseline(400_000, null), SETTINGS, file,
                cancelAfterFirstCheck()))
                .isInstanceOf(CancellationException.class);
        assertThat(dir).isEmptyDirectory();
    }

    @Test
    void cancelledMutationLeavesNoFilesBehind() throws IOException {
        Path previous = dir.resolve("s0.parquet");
        generator.generateBaseline(baseline(200_000, 3L), SETTINGS, previous, GenerationMonitor.NONE);
        Path next = dir.resolve("s1.parquet");
        Path delta = dir.resolve("deltas/s1.parquet");

        assertThatThrownBy(() -> generator.mutate(mutation(1_000_000, null), SETTINGS, previous, next, delta,
                cancelAfterFirstCheck()))
                .isInstanceOf(CancellationException.class);
        assertThat(next).doesNotExist();
        assertThat(dir.resolve("deltas")).isEmptyDirectory();
        assertThat(dir).isDirectoryNotContaining("glob:**.inprogress");
    }

    private static GenerationMonitor cancelAfterFirstCheck() {
        return new GenerationMonitor() {
            long checks;

            @Override
            public void progress(long rowsProcessed) {
            }

            @Override
            public boolean isCancelled() {
                return checks++ > 0;
            }
        };
    }

    private static BaselineSpec baseline(long records, Long seed) {
        return new BaselineSpec(records, 10, 5, 1_000_000, seed);
    }

    private static MutationSpec mutation(long firstNewUserId, Long seed) {
        return new MutationSpec(10, 5, 0.01, 0.05, firstNewUserId, seed);
    }

    private static String key(Group row) {
        return row.getLong("segment_id", 0) + "|" + row.getString("hashed_user_id", 0);
    }

    private static Set<String> keys(List<Group> rows) {
        Set<String> keys = new HashSet<>();
        rows.forEach(row -> keys.add(key(row)));
        return keys;
    }

    private static List<Group> readAll(Path file, MessageType expectedSchema) throws IOException {
        List<Group> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            assertThat(schema).isEqualTo(expectedSchema);
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    rows.add(records.read());
                }
            }
        }
        return rows;
    }
}
