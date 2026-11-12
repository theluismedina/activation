package io.synapstream.datagen.api;

import io.synapstream.datagen.generation.GenerationAlreadyRunningException;
import io.synapstream.datagen.generation.GenerationService;
import io.synapstream.datagen.generation.RunStatus;
import io.synapstream.datagen.generation.RunStatus.Trigger;
import io.synapstream.datagen.generation.ScheduleService;
import io.synapstream.datagen.generation.ScheduleStatus;
import jakarta.validation.constraints.Positive;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controls audience generation.
 * <pre>
 * GET  /api/generation             current run, last finished run, and schedule
 * POST /api/generation/start       start a run now (optional ?records=N overrides the configured count)
 * POST /api/generation/stop        stop the running run; its partial file is discarded
 * GET  /api/generation/schedule    current schedule and next fire time
 * PUT  /api/generation/schedule    change cron / zone / enabled (omitted fields are unchanged)
 * </pre>
 */
@RestController
@RequestMapping("/api/generation")
public class GenerationController {

    private final GenerationService generation;
    private final ScheduleService schedule;

    public GenerationController(GenerationService generation, ScheduleService schedule) {
        this.generation = generation;
        this.schedule = schedule;
    }

    public record GenerationOverview(RunStatus current, RunStatus lastRun, ScheduleStatus schedule) {
    }

    public record ScheduleUpdate(String cron, String zone, Boolean enabled) {
    }

    @GetMapping
    public GenerationOverview overview() {
        return new GenerationOverview(
                generation.current().orElse(null),
                generation.lastRun().orElse(null),
                schedule.status());
    }

    @PostMapping("/start")
    public ResponseEntity<RunStatus> start(@RequestParam(required = false) @Positive Long records) {
        return ResponseEntity.accepted().body(generation.start(Trigger.MANUAL, records));
    }

    @PostMapping("/stop")
    public ResponseEntity<?> stop() {
        return generation.stop()
                .<ResponseEntity<?>>map(run -> ResponseEntity.accepted().body(run))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, "No generation run is in progress")));
    }

    @GetMapping("/schedule")
    public ScheduleStatus getSchedule() {
        return schedule.status();
    }

    @PutMapping("/schedule")
    public ScheduleStatus updateSchedule(@RequestBody ScheduleUpdate update) {
        return schedule.update(update.cron(), update.zone(), update.enabled());
    }

    @ExceptionHandler(GenerationAlreadyRunningException.class)
    ProblemDetail alreadyRunning(GenerationAlreadyRunningException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        problem.setProperty("running", e.running());
        return problem;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ProblemDetail badRequest(IllegalArgumentException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
