package io.hevo.scheduler.jobs;

import io.hevo.scheduler.ExecutionStatus;
import io.hevo.scheduler.Job;
import io.hevo.scheduler.dto.ExecutionContext;
import scala.Enumeration;

import java.util.Date;

public class SimpleJob1 implements Job {
    @Override
    public Enumeration.Value execute(ExecutionContext context) {
        System.out.println("SimpleJob1::context = " + context + " Time = " + new Date());
        return ExecutionStatus.FAILED();
    }
}
