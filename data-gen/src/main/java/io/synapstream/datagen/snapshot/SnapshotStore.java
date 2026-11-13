package io.synapstream.datagen.snapshot;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.synapstream.datagen.config.DataGenProperties;
import io.synapstream.datagen.generator.SegmentPopulation;
import io.synapstream.datagen.support.AtomicFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Tracks snapshots through {@code manifest.json} in the output directory.
 * <p>
 * The manifest is the source of truth for consumers: a snapshot only counts once it is listed there. It also holds
 * the latest snapshot's {@link SegmentPopulation}, so the next mutation only adds people a segment has never had. Only the newest {@code datagen.retention} snapshots are
 * kept; older ones are removed from the manifest and their files deleted.
 */
@Component
public class SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    public static final String MANIFEST = "manifest.json";

    /**
     * @param population segments of the latest snapshot; null in manifests written before it existed
     * @param snapshots  oldest first
     */
    // Older manifests have a nextUserId field instead of population.
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Manifest(SegmentPopulation population, List<SnapshotInfo> snapshots) {

        static final Manifest EMPTY = new Manifest(null, List.of());

        public Manifest {
            snapshots = snapshots == null ? List.of() : List.copyOf(snapshots);
        }

        public Optional<SnapshotInfo> latest() {
            return snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.getLast());
        }
    }

    private final Path directory;
    private final Path manifestFile;
    private final int retention;
    private final JsonMapper json;

    public SnapshotStore(DataGenProperties properties, JsonMapper json) {
        this.directory = properties.outputDir().toAbsolutePath();
        this.manifestFile = directory.resolve(MANIFEST);
        this.retention = properties.retention();
        this.json = json;
    }

    public Path resolve(String relativeFile) {
        return directory.resolve(relativeFile);
    }

    public String relativize(Path file) {
        return directory.relativize(file.toAbsolutePath()).toString().replace('\\', '/');
    }

    /** The manifest, or an empty one if it is missing or unreadable. */
    public synchronized Manifest load() {
        if (!Files.exists(manifestFile)) {
            return Manifest.EMPTY;
        }
        try {
            return json.readValue(manifestFile.toFile(), Manifest.class);
        } catch (JacksonException e) {
            log.warn("Ignoring unreadable manifest {} ({})", manifestFile, e.getMessage());
            return Manifest.EMPTY;
        }
    }

    /** The latest snapshot in {@code manifest}, if its file still exists. */
    public Optional<SnapshotInfo> latestUsable(Manifest manifest) {
        return manifest.latest().filter(latest -> {
            if (Files.exists(resolve(latest.file()))) {
                return true;
            }
            log.warn("Latest snapshot {} is missing its file {}", latest.id(), resolve(latest.file()));
            return false;
        });
    }

    /** Appends {@code snapshot} to the manifest and deletes snapshots beyond the retention limit. */
    public synchronized void commit(SnapshotInfo snapshot, SegmentPopulation population) throws IOException {
        List<SnapshotInfo> all = new ArrayList<>(load().snapshots());
        all.add(snapshot);
        int drop = Math.max(0, all.size() - retention);
        List<SnapshotInfo> dropped = List.copyOf(all.subList(0, drop));
        List<SnapshotInfo> kept = List.copyOf(all.subList(drop, all.size()));

        // Write the manifest first so it never lists a file that has already been deleted.
        AtomicFiles.writeJson(json, manifestFile, new Manifest(population, kept));
        for (SnapshotInfo old : dropped) {
            delete(old.file());
            if (old.deltaFile() != null) {
                delete(old.deltaFile());
            }
        }
    }

    private void delete(String relativeFile) {
        try {
            Files.deleteIfExists(resolve(relativeFile));
        } catch (IOException e) {
            log.warn("Could not delete expired snapshot file {}", relativeFile, e);
        }
    }
}
