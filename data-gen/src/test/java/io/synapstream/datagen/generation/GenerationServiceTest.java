package io.synapstream.datagen.generation;

import io.synapstream.datagen.TestProperties;
import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generation.RunStatus.Mode;
import io.synapstream.datagen.generation.RunStatus.State;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import io.synapstream.datagen.generator.AudienceGenerator;
import io.synapstream.datagen.snapshot.SnapshotInfo;
import io.synapstream.datagen.snapshot.SnapshotInfo.Kind;
import io.synapstream.datagen.snapshot.SnapshotStore;
import io.synapstream.datagen.snapshot.SnapshotStore.Manifest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class GenerationServiceTest {

    @TempDir
    Path dir;

    private GenerationService service;
    private SnapshotStore store;

    @AfterEach
    void tearDown() throws InterruptedException {
        service.destroy();
    }

    @Test
    void firstRunIsBaselineThenRunsMutateTheLatestSnapshot() {
        init(3);

        RunStatus first = runToCompletion(false, null);
        assertThat(first.mode()).isEqualTo(Mode.FULL);
        assertThat(first.state()).isEqualTo(State.COMPLETED);
        assertThat(first.recordsProcessed()).isEqualTo(10_000);
        assertThat(first.deltaFile()).isNull();

        RunStatus second = runToCompletion(false, null);
        assertThat(second.mode()).isEqualTo(Mode.MUTATION);
        assertThat(second.state()).isEqualTo(State.COMPLETED);
        assertThat(second.baseSnapshot()).isEqualTo(first.id());
        assertThat(second.added() + second.removed()).isBetween(100L, 500L);
        assertThat(Path.of(second.deltaFile())).exists();

        RunStatus third = runToCompletion(false, null);
        assertThat(third.baseSnapshot()).isEqualTo(second.id());

        Manifest manifest = store.load();
        assertThat(manifest.snapshots()).extracting(SnapshotInfo::id)
                .containsExactly(first.id(), second.id(), third.id());
        assertThat(manifest.snapshots()).extracting(SnapshotInfo::sequence).containsExactly(0L, 1L, 2L);
        SnapshotInfo s1 = manifest.snapshots().get(1);
        assertThat(s1.kind()).isEqualTo(Kind.MUTATION);
        assertThat(s1.rows()).isEqualTo(10_000 + s1.added() - s1.removed());
        // The saved walks have advanced by exactly the rows each run added.
        long handedOut = Arrays.stream(manifest.population().used()).sum();
        assertThat(handedOut).isEqualTo(10_000 + s1.added() + manifest.snapshots().get(2).added());
        assertThat(manifest.population().segmentCount()).isEqualTo(50);
        assertThat(manifest.population().poolSize()).isEqualTo(TestProperties.POOL);
    }

    @Test
    void forcedFullRunIgnoresPreviousSnapshotAndCanSetSize() {
        init(3);
        runToCompletion(false, null);

        RunStatus full = runToCompletion(true, 2_000L);

        assertThat(full.mode()).isEqualTo(Mode.FULL);
        assertThat(full.recordsProcessed()).isEqualTo(2_000);
        assertThat(Arrays.stream(store.load().population().used()).sum()).isEqualTo(2_000);
    }

    @Test
    void recordCountRequiresFullMode() {
        init(3);

        assertThatThrownBy(() -> service.start(Trigger.MANUAL, false, 5L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(service.current()).isEmpty();
    }

    @Test
    void impossibleBaselineSizeIsRejectedUpFront() {
        init(3);

        // 50 segments x 10M people = 500M unique pairs
        assertThatThrownBy(() -> service.start(Trigger.MANUAL, true, 500_000_001L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(service.current()).isEmpty();
    }

    @Test
    void manifestWithoutSegmentStateStartsANewBaseline() throws IOException {
        init(3);
        RunStatus first = runToCompletion(false, null);
        // Rewrite the manifest the way earlier versions did: nextUserId instead of the segment population.
        Path manifest = Path.of(first.file()).resolveSibling(SnapshotStore.MANIFEST);
        String legacy = Files.readString(manifest).replaceFirst("\"population\"\\s*:\\s*\\{[^}]*\\}", "\"nextUserId\":5");
        Files.writeString(manifest, legacy);
        assertThat(store.load().population()).isNull();

        RunStatus next = runToCompletion(false, null);

        assertThat(next.mode()).isEqualTo(Mode.FULL);
        assertThat(store.load().population()).isNotNull();
    }

    @Test
    void missingSnapshotFileFallsBackToBaseline() throws IOException {
        init(3);
        RunStatus first = runToCompletion(false, null);
        Files.delete(Path.of(first.file()));

        assertThat(runToCompletion(false, null).mode()).isEqualTo(Mode.FULL);
    }

    @Test
    void retentionDeletesOldSnapshotsAndTheirDeltas() {
        init(2);
        RunStatus s0 = runToCompletion(false, null);
        RunStatus s1 = runToCompletion(false, null);
        RunStatus s2 = runToCompletion(false, null);
        RunStatus s3 = runToCompletion(false, null);

        assertThat(Path.of(s0.file())).doesNotExist();
        assertThat(Path.of(s1.file())).doesNotExist();
        assertThat(Path.of(s1.deltaFile())).doesNotExist();
        assertThat(Path.of(s2.file())).exists();
        assertThat(Path.of(s2.deltaFile())).exists();
        assertThat(Path.of(s3.file())).exists();
        List<SnapshotInfo> kept = store.load().snapshots();
        assertThat(kept).extracting(SnapshotInfo::id).containsExactly(s2.id(), s3.id());
    }

    @Test
    void rejectsSecondRunAndCanBeStopped() {
        init(3);

        RunStatus started = service.start(Trigger.MANUAL, true, 400_000_000L);
        assertThatThrownBy(() -> service.start(Trigger.SCHEDULED, false, null))
                .isInstanceOf(GenerationAlreadyRunningException.class);

        assertThat(service.stop()).hasValueSatisfying(s -> assertThat(s.state()).isEqualTo(State.STOPPING));

        await().atMost(Duration.ofSeconds(30)).until(() -> service.current().isEmpty());
        RunStatus last = service.lastRun().orElseThrow();
        assertThat(last.id()).isEqualTo(started.id());
        assertThat(last.state()).isEqualTo(State.CANCELLED);
        assertThat(dir.resolve("audiences")).isEmptyDirectory();
        assertThat(store.load().snapshots()).isEmpty();
        assertThat(service.stop()).isEmpty();
    }

    private RunStatus runToCompletion(boolean forceFull, Long records) {
        String id = service.start(Trigger.MANUAL, forceFull, records).id();
        await().atMost(Duration.ofSeconds(30))
                .until(() -> service.lastRun().filter(r -> r.id().equals(id)).isPresent());
        return service.lastRun().orElseThrow();
    }

    private void init(int retention) {
        DataGenProperties props = TestProperties.create(dir, retention);
        store = new SnapshotStore(props, JsonMapper.builder().build());
        service = new GenerationService(props, new AudienceGenerator(), store);
    }
}
