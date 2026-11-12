package io.synapstream.datagen.generation;

public class GenerationAlreadyRunningException extends RuntimeException {

    private final RunStatus running;

    public GenerationAlreadyRunningException(RunStatus running) {
        super("Generation run " + running.id() + " is already in progress");
        this.running = running;
    }

    public RunStatus running() {
        return running;
    }
}
