package com.hevodata.scheduler.jobs;

import com.hevodata.scheduler.ExecutionStatus;
import com.hevodata.scheduler.Job;
import com.hevodata.scheduler.dto.ExecutionContext;
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

    @Override
    public long maxRunTime() {
        return 100 * 60;
    }
}
