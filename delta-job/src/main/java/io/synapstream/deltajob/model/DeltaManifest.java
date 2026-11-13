package io.synapstream.deltajob.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * {@code manifest.json} of one delta: the claim check handed to the activation workflow.
 * <p>
 * A delta lives in its own directory, {@code <deltaId>/}, holding {@link #file()} and this manifest. The directory is
 * moved into place only once both are complete, so a delta that exists is complete, and it never changes afterwards.
 * <p>
 * Rows in {@link #file()} are sorted by (customer_id, segment_id, op, hashed_user_id), so each customer's changes are
 * one contiguous range, described by {@link #customers()}. A consumer can deliver one customer by reading only
 * {@code [firstRow, firstRow + rows)}.
 *
 * @param formatVersion bumped on incompatible changes to this manifest or the delta file
 * @param deltaId       unique and deterministic for (base, target, kind); safe to use as a workflow id
 * @param kind          {@link Kind#INCREMENTAL} for changes since {@code base}; {@link Kind#FULL} for the whole
 *                      target audience (all ADD) with no base
 * @param base          snapshot the changes are relative to; null for a FULL delta
 * @param target        snapshot the destinations should end up matching
 * @param createdAt     ISO-8601 instant
 * @param file          delta file, relative to this delta's directory
 * @param sha256        hex SHA-256 of {@code file}
 * @param customers     per-customer row ranges in {@code file}, by ascending customer id; only customers with changes
 * @param verification  result of checking the delta against data-gen's expected delta; null if not checked
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DeltaManifest(
        int formatVersion,
        String deltaId,
        Kind kind,
        SnapshotRef base,
        SnapshotRef target,
        String createdAt,
        String file,
        long rows,
        long added,
        long removed,
        String sha256,
        List<CustomerRange> customers,
        Verification verification) {

    public static final int FORMAT_VERSION = 1;
    public static final String FILE = "manifest.json";
    public static final String DATA_FILE = "changes.parquet";

    public enum Kind {
        /** Adds and removes that turn {@code base} into {@code target}. */
        INCREMENTAL,
        /**
         * Every row of {@code target}, as ADD. Destinations should replace the audience rather than apply it on top of
         * what they have, since users they hold that aren't in the target are not listed as REMOVE.
         */
        FULL
    }

    /** Rows {@code [firstRow, firstRow + rows)} of the delta file belong to {@code customerId}. */
    public record CustomerRange(long customerId, long firstRow, long rows, long added, long removed) {
    }

    /**
     * @param expectedDeltaFile data-gen's expected delta, relative to the audiences directory
     * @param missing           expected rows absent from this delta
     * @param unexpected        rows in this delta that data-gen didn't produce
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Verification(String expectedDeltaFile, long missing, long unexpected) {

        @JsonProperty("matched")
        public boolean matched() {
            return missing == 0 && unexpected == 0;
        }
    }

    public static String deltaId(Kind kind, SnapshotRef base, SnapshotRef target) {
        // Zero-padded sequence first, so directory listings sort in delivery order.
        String prefix = "%06d-%s".formatted(target.sequence(), target.id());
        return kind == Kind.FULL ? prefix + "-full" : prefix + "-from-" + base.id();
    }
}
