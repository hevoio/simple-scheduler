package io.hevo.scheduler.code.model;

import io.hevo.scheduler.core.jdbc.TaskMapper;
import io.hevo.scheduler.core.model.CronTaskDetails;
import io.hevo.scheduler.core.model.RepeatableTaskDetails;
import io.hevo.scheduler.dto.task.CronTask;
import io.hevo.scheduler.dto.task.RepeatableTask;
import io.hevo.scheduler.util.Util;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ExecutionTimesTest {

    @Test @Ignore
    public void testRepeatable() {
        RepeatableTask repeatableTask = new RepeatableTask("NS-1", "Key-1", Duration.apply(50, TimeUnit.SECONDS), "io.hevo.scheduler.jobs.SimpleJob1");
        RepeatableTaskDetails repeatableTaskDetails = (RepeatableTaskDetails) TaskMapper.toTaskDetails(repeatableTask);
        // The first execution is immediate (now)
        assertInRange(repeatableTaskDetails.nextExecutionTime(), -2, 2);
        // Next, relative to the current time
        assertInRange(repeatableTaskDetails.calculateNextExecutionTime(new Date()), 48, 52);
        // Relative to the time in the past -> 5 seconds from now
        assertInRange(repeatableTaskDetails.calculateNextExecutionTime(Util.nowWithDelta(-52)), 3, 7);
        // Relative to some time in the future
        assertInRange(repeatableTaskDetails.calculateNextExecutionTime(Util.nowWithDelta(1000)), 1048, 1052);
    }

    @Test
    public void testCron() {
        CronTask cronTask = new CronTask("NS-1", "Key-1", "0/30 * * * * ?", "io.hevo.scheduler.jobs.SimpleJob1");
        CronTaskDetails cronTaskDetails = (CronTaskDetails) TaskMapper.toTaskDetails(cronTask);
        // The first execution is immediate (now)
        assertInRange(cronTaskDetails.nextExecutionTime(), -2, 2);

        Date referenceDate = new Date(1747983551000L);  // May 23 2025 12:29:11 (Future)
        Assert.assertEquals(new Date(1747983570000L), cronTaskDetails.calculateNextExecutionTime(referenceDate));   // May 23 2025 12:29:30

        referenceDate = new Date(1590217151000L);  // May 23 2020 12:29:11 (Past)
        assertInRange(cronTaskDetails.calculateNextExecutionTime(referenceDate), 3, 7);

    }

    private void assertInRange(Date toCheck, int afterSeconds, int beforeSeconds) {
        Assert.assertTrue(Util.nowWithDelta(afterSeconds).before(toCheck) && Util.nowWithDelta(beforeSeconds).after(toCheck));
    }
}
