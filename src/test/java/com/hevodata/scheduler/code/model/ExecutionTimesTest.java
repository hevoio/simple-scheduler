package com.hevodata.scheduler.code.model;

import com.hevodata.scheduler.core.jdbc.TaskMapper;
import com.hevodata.scheduler.core.model.CronTaskDetails;
import com.hevodata.scheduler.core.model.RepeatableTaskDetails;
import com.hevodata.scheduler.dto.task.CronTask;
import com.hevodata.scheduler.dto.task.RepeatableTask;
import com.hevodata.scheduler.jobs.SampleInstantaneousJob;
import com.hevodata.scheduler.util.Util;
import org.junit.Assert;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ExecutionTimesTest {

    @Test
    public void testRepeatable() {
        RepeatableTask repeatableTask = new RepeatableTask("NS-1", "Key-1", Duration.apply(50, TimeUnit.SECONDS), SampleInstantaneousJob.class.getCanonicalName());
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
        // todo : Add test cases for DLS
        CronTask cronTask = new CronTask("NS-1", "Key-1", "0/30 * * * * ?", ZoneId.of("UTC"), SampleInstantaneousJob.class.getCanonicalName());
        CronTaskDetails cronTaskDetails = (CronTaskDetails) TaskMapper.toTaskDetails(cronTask);
        // The first execution is immediate (now)
        assertInRange(cronTaskDetails.nextExecutionTime(), -2, 2);

        Date referenceDate = new Date(1747983551000L);  // May 23 2025 12:29:11 (Future)
        Assert.assertEquals(new Date(1747983570000L), cronTaskDetails.calculateNextExecutionTime(referenceDate));   // May 23 2025 12:29:30

        referenceDate = new Date(1590217151000L);  // May 23 2020 12:29:11 (Past)
        assertInRange(cronTaskDetails.calculateNextExecutionTime(referenceDate), 3, 7);

        // Once every 8 hours
        cronTask = new CronTask("NS-1", "Key-1", "0 0 0/8 1/1 * ? *", ZoneId.of("UTC"), SampleInstantaneousJob.class.getCanonicalName());
        cronTaskDetails = (CronTaskDetails) TaskMapper.toTaskDetails(cronTask);
        Date nextExecutionTime = cronTaskDetails.calculateNextExecutionTime(new Date());
        long diff = Util.millisToSeconds(cronTaskDetails.calculateNextExecutionTime(nextExecutionTime).getTime() - nextExecutionTime.getTime());
        Assert.assertTrue(Math.abs(diff - 8 * 3600) <= 2);
    }

    private void assertInRange(Date toCheck, int afterSeconds, int beforeSeconds) {
        Assert.assertTrue(Util.nowWithDelta(afterSeconds).before(toCheck) && Util.nowWithDelta(beforeSeconds).after(toCheck));
    }
}
