package io.synapstream.deltajob.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * The parts of data-gen's {@code manifest.json} the delta-job reads. data-gen owns the file; this is a read-only
 * view that ignores fields it doesn't need.
 *
 * @param snapshots oldest first; file paths are relative to the audiences directory
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AudienceManifest(List<Snapshot> snapshots) {

    public static final String FILE = "manifest.json";

    public AudienceManifest {
        snapshots = snapshots == null ? List.of() : List.copyOf(snapshots);
    }

    /**
     * @param parentId  snapshot this one was mutated from; null for a fresh baseline
     * @param deltaFile data-gen's expected delta relative to the parent; null for a fresh baseline
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Snapshot(String id, long sequence, String kind, String parentId, String file, String deltaFile,
                           long rows) {

        public SnapshotRef ref() {
            return new SnapshotRef(id, sequence);
        }
    }

    /** Reads the manifest from {@code audiencesDir}; empty if data-gen hasn't written one yet. */
    public static AudienceManifest load(Path audiencesDir, ObjectMapper json) throws IOException {
        Path file = audiencesDir.resolve(FILE);
        return Files.exists(file) ? json.readValue(file.toFile(), AudienceManifest.class) : new AudienceManifest(null);
    }

    public Optional<Snapshot> latest() {
        return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.getLast());
    }

    public Optional<Snapshot> find(String id) {
        return snapshots.stream().filter(s -> s.id().equals(id)).findFirst();
    }
}
