package io.synapstream.datagen.config;

import jakarta.validation.Valid;
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
 * @param recordCount         number of audience rows to write per run (overridable per manual run)
 * @param customerCount       number of distinct customers (tenants); ids are {@code 1..customerCount}
 * @param segmentsPerCustomer segments owned by each customer; segment ids are globally unique
 * @param userPoolSize        number of distinct users that hashed ids are drawn from. Keeping this stable
 *                            across runs means consecutive files overlap, which is what a delta job needs
 * @param seed                optional RNG seed; with the same seed and settings a run is reproducible
 * @param outputDir           directory the Parquet files are written to
 * @param compression         Parquet compression codec
 * @param rowGroupSize        target Parquet row group size
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
        @DefaultValue @Valid Schedule schedule) {

    /**
     * @param cron    Spring 6-field cron expression (second minute hour day month weekday)
     * @param zone    time zone the cron expression is evaluated in; defaults to the system zone
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
