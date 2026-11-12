package io.synapstream.datagen.generation;

import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ScheduledFuture;

/**
 * Owns the generation schedule.
 * <p>
 * Changes made through the API are saved to {@code datagen.schedule.state-file} and restored on startup.
 * If that file is missing or unreadable, the {@code datagen.schedule.*} properties are used instead.
 */
@Service
public class ScheduleService {

    private static final Logger log = LoggerFactory.getLogger(ScheduleService.class);

    /** On-disk form of the schedule. */
    record SavedSchedule(String cron, String zone, boolean enabled) {
    }

    private final TaskScheduler scheduler;
    private final GenerationService generation;
    private final JsonMapper json;
    private final Path stateFile;

    private String cron;
    private ZoneId zone;
    private boolean enabled;
    private ScheduledFuture<?> scheduled;

    public ScheduleService(TaskScheduler scheduler, GenerationService generation, DataGenProperties properties,
                           JsonMapper json) {
        this.scheduler = scheduler;
        this.generation = generation;
        this.json = json;
        DataGenProperties.Schedule defaults = properties.schedule();
        this.stateFile = defaults.stateFile().toAbsolutePath();
        this.cron = defaults.cron();
        this.zone = defaults.zone();
        this.enabled = defaults.enabled();
        restoreSaved();
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void activate() {
        reschedule();
    }

    public synchronized ScheduleStatus status() {
        ZonedDateTime next = enabled ? CronExpression.parse(cron).next(ZonedDateTime.now(zone)) : null;
        return new ScheduleStatus(cron, zone.getId(), enabled, next != null ? next.toInstant() : null);
    }

    /**
     * Updates and saves the schedule; null arguments keep their current value.
     *
     * @throws IllegalArgumentException if the cron expression or zone is invalid
     * @throws IllegalStateException    if the schedule cannot be saved; the current schedule is left unchanged
     */
    public synchronized ScheduleStatus update(String newCron, String newZone, Boolean newEnabled) {
        String updatedCron = newCron != null ? newCron : cron;
        ZoneId updatedZone = newZone != null ? parseZone(newZone) : zone;
        boolean updatedEnabled = newEnabled != null ? newEnabled : enabled;
        if (!CronExpression.isValidExpression(updatedCron)) {
            throw new IllegalArgumentException("Invalid cron expression: '" + updatedCron + "'");
        }

        // Save first so the running schedule never diverges from what a restart would restore.
        save(new SavedSchedule(updatedCron, updatedZone.getId(), updatedEnabled));
        cron = updatedCron;
        zone = updatedZone;
        enabled = updatedEnabled;
        reschedule();
        return status();
    }

    private void restoreSaved() {
        if (!Files.exists(stateFile)) {
            log.info("No saved schedule at {}; using configured defaults", stateFile);
            return;
        }
        try {
            SavedSchedule saved = json.readValue(stateFile.toFile(), SavedSchedule.class);
            if (saved.cron() == null || !CronExpression.isValidExpression(saved.cron())) {
                throw new IllegalArgumentException("invalid cron '" + saved.cron() + "'");
            }
            ZoneId savedZone = saved.zone() != null ? parseZone(saved.zone()) : zone;
            cron = saved.cron();
            zone = savedZone;
            enabled = saved.enabled();
            log.info("Restored saved schedule from {}", stateFile);
        } catch (JacksonException | IllegalArgumentException e) {
            log.warn("Ignoring unreadable schedule file {} ({}); using configured defaults", stateFile, e.getMessage());
        }
    }

    private void save(SavedSchedule schedule) {
        Path temp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            Files.createDirectories(stateFile.getParent());
            json.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), schedule);
            try {
                Files.move(temp, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException | JacksonException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // best effort
            }
            throw new IllegalStateException("Could not save schedule to " + stateFile, e);
        }
    }

    private static ZoneId parseZone(String zone) {
        try {
            return ZoneId.of(zone);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("Invalid time zone: '" + zone + "'");
        }
    }

    private void reschedule() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
        if (enabled) {
            scheduled = scheduler.schedule(this::fire, new CronTrigger(cron, zone));
            log.info("Generation scheduled with cron '{}' ({}); next run at {}", cron, zone, status().nextRun());
        } else {
            log.info("Scheduled generation disabled");
        }
    }

    private void fire() {
        try {
            generation.start(Trigger.SCHEDULED, null);
        } catch (GenerationAlreadyRunningException e) {
            log.warn("Skipping scheduled generation: run {} is still in progress", e.running().id());
        } catch (RuntimeException e) {
            log.error("Scheduled generation failed to start", e);
        }
    }
}
