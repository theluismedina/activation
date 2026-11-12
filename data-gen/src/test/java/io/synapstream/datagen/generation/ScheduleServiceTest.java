package io.synapstream.datagen.generation;

import io.synapstream.datagen.TestProperties;
import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generator.AudienceGenerator;
import io.synapstream.datagen.snapshot.SnapshotStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScheduleServiceTest {

    @TempDir
    Path dir;

    private Path stateFile;
    private ThreadPoolTaskScheduler scheduler;
    private GenerationService generation;

    @BeforeEach
    void setUp() {
        stateFile = dir.resolve("state/schedule.json");
        scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        generation = new GenerationService(properties(), new AudienceGenerator(),
                new SnapshotStore(properties(), JsonMapper.builder().build()));
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        scheduler.shutdown();
        generation.destroy();
    }

    @Test
    void usesConfiguredDefaultsWhenNothingSaved() {
        ScheduleStatus status = newService().status();

        assertThat(status.cron()).isEqualTo("0 0 2 * * *");
        assertThat(status.zone()).isEqualTo("America/New_York");
        assertThat(status.enabled()).isTrue();
        assertThat(stateFile).doesNotExist();
    }

    @Test
    void updatesSurviveRestart() {
        newService().update("0 */15 * * * *", "UTC", false);
        assertThat(stateFile).exists();

        ScheduleStatus restored = newService().status();

        assertThat(restored.cron()).isEqualTo("0 */15 * * * *");
        assertThat(restored.zone()).isEqualTo("UTC");
        assertThat(restored.enabled()).isFalse();
    }

    @Test
    void partialUpdateKeepsOtherFields() {
        ScheduleService service = newService();
        service.update("0 30 1 * * *", "UTC", null);
        service.update(null, null, false);

        ScheduleStatus restored = newService().status();
        assertThat(restored.cron()).isEqualTo("0 30 1 * * *");
        assertThat(restored.zone()).isEqualTo("UTC");
        assertThat(restored.enabled()).isFalse();
    }

    @Test
    void invalidUpdateIsRejectedAndNothingIsSaved() {
        ScheduleService service = newService();

        assertThatThrownBy(() -> service.update("not a cron", null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.update(null, "Mars/Olympus", null)).isInstanceOf(IllegalArgumentException.class);

        assertThat(service.status().cron()).isEqualTo("0 0 2 * * *");
        assertThat(stateFile).doesNotExist();
    }

    @Test
    void corruptOrInvalidSavedFileFallsBackToDefaults() throws IOException {
        Files.createDirectories(stateFile.getParent());

        Files.writeString(stateFile, "{ not json");
        assertThat(newService().status().cron()).isEqualTo("0 0 2 * * *");

        Files.writeString(stateFile, "{\"cron\":\"bogus\",\"zone\":\"UTC\",\"enabled\":false}");
        ScheduleStatus status = newService().status();
        assertThat(status.cron()).isEqualTo("0 0 2 * * *");
        assertThat(status.enabled()).isTrue();
    }

    private ScheduleService newService() {
        return new ScheduleService(scheduler, generation, properties(), JsonMapper.builder().build());
    }

    private DataGenProperties properties() {
        DataGenProperties base = TestProperties.create(dir);
        return new DataGenProperties(base.recordCount(), base.customerCount(), base.segmentsPerCustomer(),
                base.userPoolSize(), base.seed(), base.outputDir(), base.compression(), base.rowGroupSize(),
                base.mutation(), base.retention(),
                new DataGenProperties.Schedule("0 0 2 * * *", ZoneId.of("America/New_York"), true, stateFile));
    }
}
