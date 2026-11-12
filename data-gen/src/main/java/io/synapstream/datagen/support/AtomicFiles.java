package io.synapstream.datagen.support;

import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Write-then-rename helpers, so readers never observe a partially written file.
 */
public final class AtomicFiles {

    private AtomicFiles() {
    }

    public static void moveIntoPlace(Path temp, Path target) throws IOException {
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Writes {@code value} as pretty-printed JSON to {@code target}, replacing it atomically. */
    public static void writeJson(JsonMapper json, Path target, Object value) throws IOException {
        Files.createDirectories(target.toAbsolutePath().getParent());
        Path temp = target.resolveSibling(target.getFileName() + ".tmp");
        boolean moved = false;
        try {
            json.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), value);
            moveIntoPlace(temp, target);
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(temp);
            }
        }
    }
}
