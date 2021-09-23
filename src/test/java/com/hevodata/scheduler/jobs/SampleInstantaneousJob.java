package com.hevodata.scheduler.jobs;

import com.hevodata.scheduler.ExecutionStatus;
import com.hevodata.scheduler.Job;
import com.hevodata.scheduler.dto.ExecutionContext;
import scala.Enumeration;

public class SampleInstantaneousJob implements Job {

    @Override
    public Enumeration.Value execute(ExecutionContext context) {

        return ExecutionStatus.FAILED();
    }

    @Override
    public long maxRunTime() {
        return 60;
    }
}
