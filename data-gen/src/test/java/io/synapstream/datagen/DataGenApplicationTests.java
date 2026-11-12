package io.synapstream.datagen;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class DataGenApplicationTests {

    @TempDir
    static Path dir;

    @DynamicPropertySource
    static void isolateFiles(DynamicPropertyRegistry registry) {
        registry.add("datagen.output-dir", () -> dir.resolve("audiences").toString());
        registry.add("datagen.schedule.state-file", () -> dir.resolve("schedule.json").toString());
    }

    @Autowired
    MockMvc mvc;

    @Test
    void scheduleCanBeReadAndUpdated() throws Exception {
        mvc.perform(get("/api/generation/schedule"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cron").value("0 0 2 * * *"))
                .andExpect(jsonPath("$.enabled").value(true))
                .andExpect(jsonPath("$.nextRun").isNotEmpty());

        mvc.perform(put("/api/generation/schedule").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"cron\":\"0 30 1 * * MON-FRI\",\"zone\":\"UTC\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cron").value("0 30 1 * * MON-FRI"))
                .andExpect(jsonPath("$.zone").value("UTC"));

        mvc.perform(put("/api/generation/schedule").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"enabled\":false}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled").value(false))
                .andExpect(jsonPath("$.nextRun").doesNotExist());
        assertThat(dir.resolve("schedule.json")).content().contains("0 30 1 * * MON-FRI");

        mvc.perform(put("/api/generation/schedule").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"cron\":\"not a cron\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void stopWithNothingRunningIsConflict() throws Exception {
        mvc.perform(post("/api/generation/stop")).andExpect(status().isConflict());
    }

    @Test
    void startRejectsNonPositiveRecordCount() throws Exception {
        mvc.perform(post("/api/generation/start").param("records", "0")).andExpect(status().isBadRequest());
    }
}
