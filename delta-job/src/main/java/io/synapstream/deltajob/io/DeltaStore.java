package io.synapstream.deltajob.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.synapstream.deltajob.model.DeltaManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The delta-job's output directory: one sub-directory per committed delta, named by its id.
 * <p>
 * A delta is built in {@code <deltaId>.inprogress/} and renamed to {@code <deltaId>/} once its data file and manifest
 * are both written, so readers only ever see complete deltas. Left-over {@code .inprogress} directories from a
 * crashed run are deleted on the next run.
 */
public final class DeltaStore {

    private static final Logger log = LoggerFactory.getLogger(DeltaStore.class);
    private static final String IN_PROGRESS = ".inprogress";

    private final Path directory;
    private final ObjectMapper json;

    public DeltaStore(Path directory, ObjectMapper json) {
        this.directory = directory.toAbsolutePath();
        this.json = json;
    }

    public Path directory() {
        return directory;
    }

    /** Committed deltas, oldest first (by target sequence, then creation time). */
    public List<DeltaManifest> list() throws IOException {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        List<DeltaManifest> deltas = new ArrayList<>();
        try (Stream<Path> children = Files.list(directory)) {
            for (Path child : children.filter(Files::isDirectory).toList()) {
                Path manifest = child.resolve(DeltaManifest.FILE);
                if (!child.getFileName().toString().endsWith(IN_PROGRESS) && Files.exists(manifest)) {
                    deltas.add(json.readValue(manifest.toFile(), DeltaManifest.class));
                }
            }
        }
        deltas.sort(Comparator.comparingLong((DeltaManifest d) -> d.target().sequence())
                .thenComparing(DeltaManifest::createdAt));
        return deltas;
    }

    public Optional<DeltaManifest> latest() throws IOException {
        List<DeltaManifest> all = list();
        return all.isEmpty() ? Optional.empty() : Optional.of(all.getLast());
    }

    public DeltaManifest read(String deltaId) throws IOException {
        return json.readValue(directory.resolve(deltaId).resolve(DeltaManifest.FILE).toFile(), DeltaManifest.class);
    }

    public boolean exists(String deltaId) {
        return Files.exists(directory.resolve(deltaId).resolve(DeltaManifest.FILE));
    }

    /** Deletes directories left behind by runs that died before committing. */
    public void cleanUpAbandoned() throws IOException {
        if (!Files.isDirectory(directory)) {
            return;
        }
        try (Stream<Path> children = Files.list(directory)) {
            for (Path child : children.filter(p -> p.getFileName().toString().endsWith(IN_PROGRESS)).toList()) {
                log.info("Deleting abandoned {}", child.getFileName());
                deleteRecursively(child);
            }
        }
    }

    /** A fresh, empty working directory for {@code deltaId}. */
    public Path begin(String deltaId) throws IOException {
        Path work = directory.resolve(deltaId + IN_PROGRESS);
        deleteRecursively(work);
        return Files.createDirectories(work);
    }

    /** Writes {@code manifest} into {@code work} and moves the directory into place. */
    public Path commit(Path work, DeltaManifest manifest) throws IOException {
        json.writerWithDefaultPrettyPrinter().writeValue(work.resolve(DeltaManifest.FILE).toFile(), manifest);
        Path target = directory.resolve(manifest.deltaId());
        if (Files.exists(target)) {
            throw new FileAlreadyExistsException(target.toString(), null, "delta already committed");
        }
        return Files.move(work, target, StandardCopyOption.ATOMIC_MOVE);
    }

    public void abandon(Path work) {
        try {
            deleteRecursively(work);
        } catch (IOException e) {
            log.warn("Could not delete {}", work, e);
        }
    }

    public static String sha256(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(path)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
