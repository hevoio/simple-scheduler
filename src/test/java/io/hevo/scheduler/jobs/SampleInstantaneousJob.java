package io.hevo.scheduler.jobs;

import io.hevo.scheduler.ExecutionStatus;
import io.hevo.scheduler.Job;
import io.hevo.scheduler.dto.ExecutionContext;
import scala.Enumeration;

public class SampleInstantaneousJob implements Job {

    @Override
    public Enumeration.Value execute(ExecutionContext context) {
        return ExecutionStatus.FAILED();
    }
}
