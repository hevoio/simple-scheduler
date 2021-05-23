package io.hevo.scheduler;

import io.hevo.scheduler.handler.JobHandlerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class JobResolver implements JobHandlerFactory {

    private final Map<String, Job> REGISTRY = new HashMap<>();

    public void register(Job job) {
        REGISTRY.put(job.getClass().getCanonicalName(), job);
    }

    @Override
    public Optional<Job> resolve(String fqcn) {
        return Optional.ofNullable(REGISTRY.get(fqcn));
    }
}
