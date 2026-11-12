package io.synapstream.datagen.generator;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public record WriterSettings(CompressionCodecName compression, long rowGroupSizeBytes) {
}
