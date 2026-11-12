package io.synapstream.datagen.generator;

/**
 * One audience membership row.
 *
 * @param hashedUserId lowercase hex SHA-256, as ASCII bytes (kept as bytes to avoid a String per row)
 */
record Audience(long customerId, long segmentId, byte[] hashedUserId) {
}
