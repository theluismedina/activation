package io.synapstream.deltajob;

import io.synapstream.deltajob.DeltaJob.DeltaJobException;
import io.synapstream.deltajob.DeltaJob.Outcome;
import io.synapstream.deltajob.DeltaJob.Status;
import io.synapstream.deltajob.Fixtures.Change;
import io.synapstream.deltajob.Fixtures.Member;
import io.synapstream.deltajob.Fixtures.Snapshot;
import io.synapstream.deltajob.io.DeltaStore;
import io.synapstream.deltajob.model.DeltaManifest;
import io.synapstream.deltajob.model.DeltaManifest.CustomerRange;
import io.synapstream.deltajob.model.DeltaManifest.Kind;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeltaJobTest {

    private static SparkSession spark;

    @TempDir
    Path dir;

    private Fixtures dataGen;
    private Path deltas;

    @BeforeAll
    static void startSpark() {
        spark = DeltaJobApplication.session(config(Path.of("unused"), Path.of("unused")));
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @BeforeEach
    void setUp() throws IOException {
        dataGen = new Fixtures(dir.resolve("audiences"));
        deltas = dir.resolve("deltas");
    }

    @Test
    void doesNothingBeforeDataGenHasRun() throws IOException {
        assertThat(run().status()).isEqualTo(Status.NO_SNAPSHOTS);
    }

    @Test
    void firstRunSendsTheWholeAudienceAsAFullDelta() throws IOException {
        Snapshot baseline = dataGen.baseline(5, 4, 50);

        Outcome outcome = run();

        assertThat(outcome.status()).isEqualTo(Status.CREATED);
        DeltaManifest delta = outcome.delta();
        assertThat(delta.kind()).isEqualTo(Kind.FULL);
        assertThat(delta.base()).isNull();
        assertThat(delta.target().id()).isEqualTo(baseline.id());
        assertThat(delta.deltaId()).isEqualTo("000000-" + baseline.id() + "-full");
        assertThat(delta.rows()).isEqualTo(baseline.members().size()).isEqualTo(delta.added());
        assertThat(delta.removed()).isZero();
        assertThat(new HashSet<>(rows(outcome))).isEqualTo(Fixtures.diff(Set.<Member>of(), baseline.members()));
    }

    @Test
    void laterRunsSendOnlyTheChangesAndMatchDataGensExpectedDelta() throws IOException {
        Snapshot baseline = dataGen.baseline(5, 4, 50);
        run();
        Snapshot mutated = dataGen.mutate(baseline, 0.05);

        Outcome outcome = run(true);

        DeltaManifest delta = outcome.delta();
        assertThat(delta.kind()).isEqualTo(Kind.INCREMENTAL);
        assertThat(delta.base().id()).isEqualTo(baseline.id());
        assertThat(delta.target().id()).isEqualTo(mutated.id());
        assertThat(delta.deltaId()).isEqualTo("000001-" + mutated.id() + "-from-" + baseline.id());
        Set<Change> expected = Fixtures.diff(baseline.members(), mutated.members());
        assertThat(new HashSet<>(rows(outcome))).isEqualTo(expected);
        assertThat(delta.added()).isEqualTo(expected.stream().filter(c -> c.op().equals("ADD")).count());
        assertThat(delta.removed()).isEqualTo(expected.stream().filter(c -> c.op().equals("REMOVE")).count());
        assertThat(delta.verification()).isNotNull();
        assertThat(delta.verification().matched()).isTrue();
        assertThat(delta.verification().expectedDeltaFile()).isEqualTo(mutated.deltaFile());
    }

    @Test
    void rowsAreSortedAndTheManifestDescribesEachCustomersRange() throws IOException {
        Snapshot baseline = dataGen.baseline(6, 3, 40);
        run();
        dataGen.mutate(baseline, 0.1);

        Outcome outcome = run();
        List<Change> rows = rows(outcome);

        Comparator<Change> order = Comparator.comparingLong(Change::customerId).thenComparingLong(Change::segmentId)
                .thenComparing(Change::op).thenComparing(Change::user);
        assertThat(rows).isSortedAccordingTo(order);
        List<CustomerRange> ranges = outcome.delta().customers();
        assertThat(ranges).extracting(CustomerRange::customerId).isSorted().doesNotHaveDuplicates();
        long next = 0;
        for (CustomerRange range : ranges) {
            assertThat(range.firstRow()).isEqualTo(next);
            List<Change> slice = rows.subList((int) range.firstRow(), (int) (range.firstRow() + range.rows()));
            assertThat(slice).allMatch(c -> c.customerId() == range.customerId());
            assertThat(slice.stream().filter(c -> c.op().equals("ADD")).count()).isEqualTo(range.added());
            assertThat(range.added() + range.removed()).isEqualTo(range.rows());
            next += range.rows();
        }
        assertThat(next).isEqualTo(rows.size());
        assertThat(outcome.delta().sha256())
                .isEqualTo(DeltaStore.sha256(outcome.directory().resolve(outcome.delta().file())));
    }

    @Test
    void rerunningWithNothingNewIsANoOp() throws IOException {
        Snapshot baseline = dataGen.baseline(3, 2, 20);
        run();
        dataGen.mutate(baseline, 0.05);
        Outcome first = run();

        Outcome again = run();

        assertThat(again.status()).isEqualTo(Status.UP_TO_DATE);
        assertThat(again.delta().deltaId()).isEqualTo(first.delta().deltaId());
        assertThat(deltaDirectories()).hasSize(2);
    }

    @Test
    void chainsFromTheLastDeltaWhenDataGenRanSeveralTimesInBetween() throws IOException {
        Snapshot baseline = dataGen.baseline(4, 3, 30);
        run();
        Snapshot second = dataGen.mutate(baseline, 0.05);
        Snapshot third = dataGen.mutate(second, 0.05);

        Outcome outcome = run(true);

        assertThat(outcome.delta().base().id()).isEqualTo(baseline.id());
        assertThat(outcome.delta().target().id()).isEqualTo(third.id());
        assertThat(new HashSet<>(rows(outcome))).isEqualTo(Fixtures.diff(baseline.members(), third.members()));
        // data-gen only has expected deltas for single steps.
        assertThat(outcome.delta().verification()).isNull();
    }

    @Test
    void failsWithAClearMessageWhenTheBaseWasPrunedAndRecoversWithFull() throws IOException {
        Snapshot baseline = dataGen.baseline(3, 2, 20);
        run();
        Snapshot second = dataGen.mutate(baseline, 0.05);
        Snapshot third = dataGen.mutate(second, 0.05);
        dataGen.prune(2);

        assertThatThrownBy(this::run).isInstanceOf(DeltaJobException.class)
                .hasMessageContaining(baseline.id()).hasMessageContaining("--full");

        Outcome outcome = new DeltaJob(config(true, true), spark, DeltaJobApplication.objectMapper(),
                Clock.systemUTC()).run();
        assertThat(outcome.delta().kind()).isEqualTo(Kind.FULL);
        assertThat(outcome.delta().target().id()).isEqualTo(third.id());
        // The chain continues from the FULL delta.
        Snapshot fourth = dataGen.mutate(third, 0.05);
        Outcome next = run();
        assertThat(next.delta().base().id()).isEqualTo(third.id());
        assertThat(next.delta().target().id()).isEqualTo(fourth.id());
    }

    @Test
    void commitsNothingWhenTheDeltaDoesNotMatchDataGensExpectedDelta() throws IOException {
        Snapshot baseline = dataGen.baseline(3, 2, 20);
        run();
        Snapshot mutated = dataGen.mutate(baseline, 0.05);
        // Corrupt the expected delta: drop one row and add one data-gen never produced.
        Path expected = dataGen.resolve(mutated.deltaFile());
        List<Change> rows = new ArrayList<>(Fixtures.readDelta(expected));
        rows.removeFirst();
        rows.add(new Change(1, 1, "not-a-real-user", "ADD"));
        Files.delete(expected);
        Fixtures.writeDelta(expected, rows);

        assertThatThrownBy(() -> run(true)).isInstanceOf(DeltaJobException.class)
                .hasMessageContaining("1 expected rows missing, 1 unexpected rows");
        assertThat(deltaDirectories()).hasSize(1);
    }

    @Test
    void readsSettingsFromTheEnvironmentAndFlags() {
        DeltaJobConfig config = DeltaJobConfig.from(
                Map.of("DELTAJOB_AUDIENCES_DIR", "/data/audiences", "DELTAJOB_OUTPUT_DIR", "/deltas",
                        "DELTAJOB_VERIFY", "true"),
                "--full", "--partitions=4");

        assertThat(config.audiencesDir()).isEqualTo(Path.of("/data/audiences"));
        assertThat(config.outputDir()).isEqualTo(Path.of("/deltas"));
        assertThat(config.verify()).isTrue();
        assertThat(config.full()).isTrue();
        assertThat(config.shufflePartitions()).isEqualTo(4);
        assertThat(config.sparkMaster()).isEqualTo("local[*]");
        assertThatThrownBy(() -> DeltaJobConfig.from(Map.of(), "--nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private Outcome run() throws IOException {
        return run(false);
    }

    private Outcome run(boolean verify) throws IOException {
        return new DeltaJob(config(verify, false), spark, DeltaJobApplication.objectMapper(), Clock.systemUTC()).run();
    }

    private DeltaJobConfig config(boolean verify, boolean full) {
        return new DeltaJobConfig(dir.resolve("audiences"), deltas, "local[2]", 4, verify, full,
                CompressionCodecName.SNAPPY, 1024 * 1024);
    }

    private static DeltaJobConfig config(Path audiences, Path output) {
        return new DeltaJobConfig(audiences, output, "local[2]", 4, false, false,
                CompressionCodecName.SNAPPY, 1024 * 1024);
    }

    private static List<Change> rows(Outcome outcome) throws IOException {
        return Fixtures.readDelta(outcome.directory().resolve(outcome.delta().file()));
    }

    private List<Path> deltaDirectories() throws IOException {
        try (Stream<Path> children = Files.list(deltas)) {
            return children.toList();
        }
    }
}
