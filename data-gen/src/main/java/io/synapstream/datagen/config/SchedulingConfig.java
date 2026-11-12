package io.synapstream.datagen.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DataGenProperties.class)
public class SchedulingConfig {

    /**
     * Fires scheduled runs. The trigger only hands work to the generation executor, so one thread is enough.
     */
    @Bean
    ThreadPoolTaskScheduler generationTaskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("datagen-schedule-");
        return scheduler;
    }
}
