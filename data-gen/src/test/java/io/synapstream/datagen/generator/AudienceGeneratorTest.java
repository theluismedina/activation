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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CancellationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AudienceGeneratorTest {

    private final AudienceGenerator generator = new AudienceGenerator();

    @TempDir
    Path dir;

    @Test
    void writesRequestedRowsWithExpectedSchemaAndValueRanges() throws IOException {
        Path file = dir.resolve("out/audiences.parquet");
        GenerationSpec spec = spec(120_000, 42L);

        long written = generator.generate(spec, file, GenerationMonitor.NONE);

        assertThat(written).isEqualTo(120_000);
        assertThat(file).exists();
        assertThat(file.resolveSibling(file.getFileName() + AudienceGenerator.IN_PROGRESS_SUFFIX)).doesNotExist();

        List<Group> rows = readAll(file);
        assertThat(rows).hasSize(120_000);
        for (Group row : rows) {
            long customer = row.getLong("customer_id", 0);
            long segment = row.getLong("segment_id", 0);
            String hash = row.getString("hashed_user_id", 0);
            assertThat(customer).isBetween(1L, 10L);
            // Each customer owns 5 segments: customer N -> ((N-1)*5, N*5]
            assertThat(segment).isBetween((customer - 1) * 5 + 1, customer * 5);
            assertThat(hash).hasSize(64).matches("[0-9a-f]+");
        }
    }

    @Test
    void sameSeedProducesSameRows() throws IOException {
        Path a = dir.resolve("a.parquet");
        Path b = dir.resolve("b.parquet");
        generator.generate(spec(1_000, 7L), a, GenerationMonitor.NONE);
        generator.generate(spec(1_000, 7L), b, GenerationMonitor.NONE);

        assertThat(readAll(a).toString()).isEqualTo(readAll(b).toString());
    }

    @Test
    void hashIsHexSha256OfEmail() throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        String expected = HexFormat.of().formatHex(sha.digest("user123@example.com".getBytes()));

        assertThat(new String(AudienceGenerator.hashedEmail(123, sha))).isEqualTo(expected);
    }

    @Test
    void cancellationLeavesNoFileBehind() {
        Path file = dir.resolve("cancelled.parquet");
        GenerationMonitor cancelAfterFirstCheck = new GenerationMonitor() {
            long checks;

            @Override
            public void progress(long rowsWritten) {
            }

            @Override
            public boolean isCancelled() {
                return checks++ > 0;
            }
        };

        assertThatThrownBy(() -> generator.generate(spec(1_000_000, null), file, cancelAfterFirstCheck))
                .isInstanceOf(CancellationException.class);
        assertThat(dir).isEmptyDirectory();
    }

    private static GenerationSpec spec(long records, Long seed) {
        return new GenerationSpec(records, 10, 5, 1_000, seed, CompressionCodecName.SNAPPY, 8L * 1024 * 1024);
    }

    private static List<Group> readAll(Path file) throws IOException {
        List<Group> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            assertThat(schema).isEqualTo(AudienceWriteSupport.SCHEMA);
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                RecordReader<Group> records = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    rows.add(records.read());
                }
            }
        }
        assertThat(Files.size(file)).isPositive();
        return rows;
    }
}
