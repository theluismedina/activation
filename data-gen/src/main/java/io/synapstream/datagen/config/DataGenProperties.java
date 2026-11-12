package io.synapstream.datagen.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.ZoneId;

/**
 * Settings for audience generation, bound from {@code datagen.*}.
 *
 * @param recordCount         rows in a FULL (baseline) snapshot (overridable per manual full run)
 * @param customerCount       number of distinct customers (tenants); ids are {@code 1..customerCount}
 * @param segmentsPerCustomer segments owned by each customer; segment ids are globally unique
 * @param userPoolSize        number of distinct users a baseline draws from
 * @param seed                optional RNG seed; with the same seed and settings a sequence of runs is reproducible
 * @param outputDir           directory holding snapshots, expected deltas and the manifest
 * @param compression         Parquet compression codec
 * @param rowGroupSize        target Parquet row group size
 * @param mutation            how much each run after the baseline changes the audience
 * @param retention           snapshots to keep (minimum 2, so a delta job always has a previous snapshot)
 * @param schedule            initial schedule; can be changed at runtime through the API
 */
@Validated
@ConfigurationProperties("datagen")
public record DataGenProperties(
        @DefaultValue("10000000") @Positive long recordCount,
        @DefaultValue("1000") @Positive int customerCount,
        @DefaultValue("20") @Positive int segmentsPerCustomer,
        @DefaultValue("50000000") @Positive long userPoolSize,
        Long seed,
        @DefaultValue("data/audiences") @NotNull Path outputDir,
        @DefaultValue("SNAPPY") @NotNull CompressionCodecName compression,
        @DefaultValue("128MB") @NotNull DataSize rowGroupSize,
        @DefaultValue @Valid Mutation mutation,
        @DefaultValue("3") @Min(2) int retention,
        @DefaultValue @Valid Schedule schedule) {

    /**
     * Each run after the baseline changes between {@code minRate} and {@code maxRate} of the previous snapshot's
     * rows (removes plus adds), with the rate drawn uniformly per run.
     */
    public record Mutation(
            @DefaultValue("0.01") double minRate,
            @DefaultValue("0.05") double maxRate) {

        public Mutation {
            if (!(minRate > 0 && minRate <= maxRate && maxRate < 1)) {
                throw new IllegalArgumentException(
                        "datagen.mutation requires 0 < min-rate <= max-rate < 1 (got " + minRate + ", " + maxRate + ")");
            }
        }
    }

    /**
     * @param cron      Spring 6-field cron expression (second minute hour day month weekday)
     * @param zone      time zone the cron expression is evaluated in; defaults to the system zone
     * @param enabled   whether scheduled runs fire
     * @param stateFile where schedule changes made through the API are saved. When this file exists it takes
     *                  precedence over the values above, which then only act as first-start defaults
     */
    public record Schedule(
            @DefaultValue("0 0 2 * * *") @NotNull String cron,
            ZoneId zone,
            @DefaultValue("true") boolean enabled,
            @DefaultValue("data/schedule.json") @NotNull Path stateFile) {

        public Schedule {
            if (!CronExpression.isValidExpression(cron)) {
                throw new IllegalArgumentException("Invalid datagen.schedule.cron: '" + cron + "'");
            }
            if (zone == null) {
                zone = ZoneId.systemDefault();
            }
        }
    }
}
