package io.synapstream.datagen.generation;

import java.time.Instant;

/**
 * @param nextRun when the schedule fires next; null when disabled
 */
public record ScheduleStatus(String cron, String zone, boolean enabled, Instant nextRun) {
}
