package io.synapstream.deltajob;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Stands in for data-gen: writes snapshots, expected deltas and {@code manifest.json} in data-gen's layout and
 * schema, so the job can be tested without the data-gen module on the classpath.
 */
final class Fixtures {

    static final MessageType SNAPSHOT_SCHEMA = MessageTypeParser.parseMessageType("""
            message audience {
              required int64 customer_id;
              required int64 segment_id;
              required binary hashed_user_id (STRING);
            }""");
    static final MessageType DELTA_SCHEMA = MessageTypeParser.parseMessageType("""
            message audience_delta {
              required int64 customer_id;
              required int64 segment_id;
              required binary hashed_user_id (STRING);
              required binary op (ENUM);
            }""");

    record Member(long customerId, long segmentId, String user) {
    }

    record Change(long customerId, long segmentId, String user, String op) {
        Member member() {
            return new Member(customerId, segmentId, user);
        }
    }

    /** A snapshot as data-gen's manifest lists it. */
    record Snapshot(String id, long sequence, String parentId, String file, String deltaFile, Set<Member> members) {
    }

    private final Path audiencesDir;
    private final ObjectMapper json = DeltaJobApplication.objectMapper();
    private final Random random = new Random(7);
    private final List<Snapshot> manifest = new ArrayList<>();
    private long sequence;
    private long nextUser;

    Fixtures(Path audiencesDir) throws IOException {
        this.audiencesDir = Files.createDirectories(audiencesDir);
    }

    /** A fresh baseline of about {@code customers * segments * perSegment} rows. */
    Snapshot baseline(int customers, int segments, int perSegment) throws IOException {
        Set<Member> members = new HashSet<>();
        for (long c = 1; c <= customers; c++) {
            for (long s = 1; s <= segments; s++) {
                for (int u = 0; u < perSegment; u++) {
                    // Users are shared across segments, like data-gen's pool.
                    members.add(new Member(c, s, user(random.nextInt(perSegment * 3))));
                }
            }
        }
        nextUser = perSegment * 3L;
        return commit(null, members, null);
    }

    /** Removes about {@code rate} of {@code parent}'s rows and adds as many new ones, with an expected delta. */
    Snapshot mutate(Snapshot parent, double rate) throws IOException {
        Set<Member> members = new HashSet<>(parent.members());
        List<Change> changes = new ArrayList<>();
        for (Member m : parent.members()) {
            if (random.nextDouble() < rate) {
                members.remove(m);
                changes.add(new Change(m.customerId(), m.segmentId(), m.user(), "REMOVE"));
            }
        }
        List<Member> existing = new ArrayList<>(parent.members());
        int adds = Math.max(1, (int) (parent.members().size() * rate));
        for (int i = 0; i < adds; i++) {
            Member like = existing.get(random.nextInt(existing.size()));
            Member added = new Member(like.customerId(), like.segmentId(), user(nextUser++));
            members.add(added);
            changes.add(new Change(added.customerId(), added.segmentId(), added.user(), "ADD"));
        }
        return commit(parent, members, changes);
    }

    /** Drops the oldest snapshots from the manifest, as data-gen's retention does. */
    void prune(int keep) throws IOException {
        while (manifest.size() > keep) {
            Snapshot old = manifest.removeFirst();
            Files.deleteIfExists(audiencesDir.resolve(old.file()));
        }
        writeManifest();
    }

    Path resolve(String file) {
        return audiencesDir.resolve(file);
    }

    private Snapshot commit(Snapshot parent, Set<Member> members, List<Change> changes) throws IOException {
        String id = "%08x".formatted(random.nextInt());
        String file = "audiences-" + id + ".parquet";
        String deltaFile = changes == null ? null : "expected-deltas/audiences-" + id + ".parquet";
        // data-gen's write order shouldn't matter to the job, so shuffle it.
        List<Member> rows = new ArrayList<>(members);
        Collections.shuffle(rows, random);
        writeSnapshot(audiencesDir.resolve(file), rows);
        if (changes != null) {
            Files.createDirectories(audiencesDir.resolve("expected-deltas"));
            writeDelta(audiencesDir.resolve(deltaFile), changes);
        }
        Snapshot snapshot = new Snapshot(id, sequence++, parent == null ? null : parent.id(), file, deltaFile,
                Set.copyOf(members));
        manifest.add(snapshot);
        writeManifest();
        return snapshot;
    }

    private void writeManifest() throws IOException {
        List<Map<String, Object>> snapshots = new ArrayList<>();
        for (Snapshot s : manifest) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", s.id());
            entry.put("sequence", s.sequence());
            entry.put("kind", s.parentId() == null ? "FULL" : "MUTATION");
            entry.put("parentId", s.parentId());
            entry.put("createdAt", "2026-11-12T02:00:00Z");
            entry.put("file", s.file());
            entry.put("deltaFile", s.deltaFile());
            entry.put("rows", s.members().size());
            snapshots.add(entry);
        }
        json.writeValue(audiencesDir.resolve("manifest.json").toFile(),
                Map.of("nextUserId", nextUser, "snapshots", snapshots));
    }

    static void writeSnapshot(Path file, List<Member> rows) throws IOException {
        SimpleGroupFactory groups = new SimpleGroupFactory(SNAPSHOT_SCHEMA);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(SNAPSHOT_SCHEMA).build()) {
            for (Member m : rows) {
                writer.write(groups.newGroup().append("customer_id", m.customerId())
                        .append("segment_id", m.segmentId()).append("hashed_user_id", m.user()));
            }
        }
    }

    static void writeDelta(Path file, List<Change> rows) throws IOException {
        SimpleGroupFactory groups = new SimpleGroupFactory(DELTA_SCHEMA);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(DELTA_SCHEMA).build()) {
            for (Change c : rows) {
                writer.write(groups.newGroup().append("customer_id", c.customerId())
                        .append("segment_id", c.segmentId()).append("hashed_user_id", c.user())
                        .append("op", c.op()));
            }
        }
    }

    /** Every row of a delta file, in file order. */
    static List<Change> readDelta(Path file) throws IOException {
        List<Change> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                RecordReader<Group> records = new ColumnIOFactory().getColumnIO(schema)
                        .getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i = 0; i < pages.getRowCount(); i++) {
                    Group g = records.read();
                    rows.add(new Change(g.getLong("customer_id", 0), g.getLong("segment_id", 0),
                            g.getString("hashed_user_id", 0), g.getString("op", 0)));
                }
            }
        }
        return rows;
    }

    /** The changes that turn {@code from} into {@code to}. */
    static Set<Change> diff(Set<Member> from, Set<Member> to) {
        Set<Change> changes = new HashSet<>();
        from.stream().filter(m -> !to.contains(m))
                .forEach(m -> changes.add(new Change(m.customerId(), m.segmentId(), m.user(), "REMOVE")));
        to.stream().filter(m -> !from.contains(m))
                .forEach(m -> changes.add(new Change(m.customerId(), m.segmentId(), m.user(), "ADD")));
        return changes;
    }

    private static String user(long n) {
        return "%064x".formatted(n * 0x9E3779B97F4A7C15L);
    }
}
