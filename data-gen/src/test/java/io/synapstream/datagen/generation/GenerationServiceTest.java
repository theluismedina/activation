package io.synapstream.datagen.generation;

import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generation.RunStatus.State;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import io.synapstream.datagen.generator.AudienceGenerator;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.unit.DataSize;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class GenerationServiceTest {

    @TempDir
    Path dir;

    private GenerationService service;

    @AfterEach
    void tearDown() throws InterruptedException {
        service.destroy();
    }

    @Test
    void runsToCompletionAndRecordsLastRun() {
        service = newService();

        RunStatus started = service.start(Trigger.MANUAL, 10_000L);
        assertThat(started.state()).isEqualTo(State.RUNNING);

        await().atMost(Duration.ofSeconds(30)).until(() -> service.current().isEmpty());
        RunStatus last = service.lastRun().orElseThrow();
        assertThat(last.id()).isEqualTo(started.id());
        assertThat(last.state()).isEqualTo(State.COMPLETED);
        assertThat(last.recordsWritten()).isEqualTo(10_000);
        assertThat(Path.of(last.file())).exists();
    }

    @Test
    void rejectsSecondRunAndCanBeStopped() {
        service = newService();

        RunStatus started = service.start(Trigger.MANUAL, 500_000_000L);
        assertThatThrownBy(() -> service.start(Trigger.SCHEDULED, null))
                .isInstanceOf(GenerationAlreadyRunningException.class);

        assertThat(service.stop()).hasValueSatisfying(s -> assertThat(s.state()).isEqualTo(State.STOPPING));

        await().atMost(Duration.ofSeconds(30)).until(() -> service.current().isEmpty());
        RunStatus last = service.lastRun().orElseThrow();
        assertThat(last.id()).isEqualTo(started.id());
        assertThat(last.state()).isEqualTo(State.CANCELLED);
        assertThat(dir).isEmptyDirectory();
        assertThat(service.stop()).isEmpty();
    }

    private GenerationService newService() {
        var props = new DataGenProperties(1_000, 10, 5, 1_000, null, dir, CompressionCodecName.SNAPPY,
                DataSize.ofMegabytes(8), new DataGenProperties.Schedule("0 0 2 * * *", null, false, dir.resolve("schedule.json")));
        return new GenerationService(props, new AudienceGenerator());
    }
}
