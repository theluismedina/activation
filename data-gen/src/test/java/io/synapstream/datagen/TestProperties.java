package io.synapstream.datagen;

import io.synapstream.datagen.config.DataGenProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.util.unit.DataSize;

import java.nio.file.Path;
import java.time.ZoneId;

/** Small, fast settings for tests: 10k-row baseline, 10 customers x 5 segments, 10M-user pool. */
public final class TestProperties {

    public static final long POOL = 10_000_000;

    private TestProperties() {
    }

    public static DataGenProperties create(Path dir) {
        return create(dir, 3);
    }

    public static DataGenProperties create(Path dir, int retention) {
        return new DataGenProperties(10_000, 10, 5, POOL, null, dir.resolve("audiences"),
                CompressionCodecName.SNAPPY, DataSize.ofMegabytes(8),
                new DataGenProperties.Mutation(0.01, 0.05), retention,
                new DataGenProperties.Schedule("0 0 2 * * *", ZoneId.of("America/New_York"), true,
                        dir.resolve("schedule.json")));
    }
}
