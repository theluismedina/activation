package io.synapstream.deltajob.model;

/** A data-gen snapshot, identified by its id and its position in data-gen's sequence. */
public record SnapshotRef(String id, long sequence) {
}
