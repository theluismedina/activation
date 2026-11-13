package io.synapstream.deltajob;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.nio.file.Path;
import java.util.Map;

/**
 * Settings for one run, from environment variables, overridable by command-line flags.
 *
 * <pre>
 * DELTAJOB_AUDIENCES_DIR      --audiences=DIR   data-gen's output (holds manifest.json)  [data/audiences]
 * DELTAJOB_OUTPUT_DIR         --output=DIR      where deltas are written                  [data/deltas]
 * DELTAJOB_SPARK_MASTER       --master=URL      Spark master                              [local[*]]
 * DELTAJOB_SHUFFLE_PARTITIONS --partitions=N    spark.sql.shuffle.partitions              [16]
 * DELTAJOB_VERIFY=true        --verify          check against data-gen's expected delta   [false]
 *                             --full            send the whole latest snapshot as a FULL delta
 * </pre>
 */
public record DeltaJobConfig(
        Path audiencesDir,
        Path outputDir,
        String sparkMaster,
        int shufflePartitions,
        boolean verify,
        boolean full,
        CompressionCodecName compression,
        long rowGroupSizeBytes) {

    public DeltaJobConfig {
        if (shufflePartitions < 1) {
            throw new IllegalArgumentException("Shuffle partitions must be at least 1");
        }
    }

    public static DeltaJobConfig from(Map<String, String> env, String... args) {
        Path audiences = Path.of(env.getOrDefault("DELTAJOB_AUDIENCES_DIR", "data/audiences"));
        Path output = Path.of(env.getOrDefault("DELTAJOB_OUTPUT_DIR", "data/deltas"));
        String master = env.getOrDefault("DELTAJOB_SPARK_MASTER", "local[*]");
        int partitions = Integer.parseInt(env.getOrDefault("DELTAJOB_SHUFFLE_PARTITIONS", "16"));
        boolean verify = Boolean.parseBoolean(env.getOrDefault("DELTAJOB_VERIFY", "false"));
        boolean full = false;
        for (String arg : args) {
            int eq = arg.indexOf('=');
            String name = eq < 0 ? arg : arg.substring(0, eq);
            String value = eq < 0 ? null : arg.substring(eq + 1);
            switch (name) {
                case "--audiences" -> audiences = Path.of(required(name, value));
                case "--output" -> output = Path.of(required(name, value));
                case "--master" -> master = required(name, value);
                case "--partitions" -> partitions = Integer.parseInt(required(name, value));
                case "--verify" -> verify = true;
                case "--full" -> full = true;
                default -> throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }
        return new DeltaJobConfig(audiences, output, master, partitions, verify, full, CompressionCodecName.SNAPPY,
                64L * 1024 * 1024);
    }

    private static String required(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " needs a value, e.g. " + name + "=...");
        }
        return value;
    }
}
