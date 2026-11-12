package io.synapstream.datagen.generator;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Everything needed to produce one audience file.
 *
 * @param seed RNG seed, or {@code null} for a random one
 */
public record GenerationSpec(
        long recordCount,
        int customerCount,
        int segmentsPerCustomer,
        long userPoolSize,
        Long seed,
        CompressionCodecName compression,
        long rowGroupSizeBytes) {

    public GenerationSpec {
        if (recordCount <= 0 || customerCount <= 0 || segmentsPerCustomer <= 0 || userPoolSize <= 0) {
            throw new IllegalArgumentException("counts and sizes must be positive");
        }
    }
}
