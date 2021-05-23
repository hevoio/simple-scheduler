package io.hevo.scheduler.jobs;

import io.hevo.scheduler.ExecutionStatus;
import io.hevo.scheduler.Job;
import io.hevo.scheduler.dto.ExecutionContext;
import scala.Enumeration;

import java.util.Random;

public class SampleLongRunningJob implements Job {
    private static final Random RANDOM = new Random();
    @Override
    public Enumeration.Value execute(ExecutionContext context) {
        if("FAIL_IMMEDIATELY".equals(context.parameters())) {
            throw new RuntimeException("Job Failure");
        }
        long sleepTime = 50_000 + RANDOM.nextInt(30 - 2);
        try {
            Thread.sleep(sleepTime);
        }
        catch (InterruptedException iEx) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Work interrupted");
        }
        return ExecutionStatus.SUCCEEDED();
    }
}
