package io.hevo.scheduler;

import io.hevo.scheduler.config.SchedulerConfig;
import io.hevo.scheduler.config.WorkConfig;
import io.hevo.scheduler.core.jdbc.TaskRepository;
import io.hevo.scheduler.core.model.TaskDetails;
import io.hevo.scheduler.core.model.TaskStatus;
import io.hevo.scheduler.dto.task.CronTask;
import io.hevo.scheduler.dto.task.RepeatableTask;
import io.hevo.scheduler.helpers.MySqlHelper;
import io.hevo.scheduler.helpers.profile.EmbeddedMySql;
import io.hevo.scheduler.helpers.profile.LocalMySql;
import io.hevo.scheduler.jobs.SampleLongRunningJob;
import io.hevo.scheduler.jobs.SampleInstantaneousJob;
import io.hevo.scheduler.lock.Lock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.Iterator;
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestScheduler {

    private static final String TABLE_PREFIX = "mysql_";
    private static final String NS = "NS";
    private static final String JOB_LONG_RUNNING_FQCN = SampleLongRunningJob.class.getCanonicalName();
    private static final String JOB_INSTANTANEOUS_FQCN = SampleInstantaneousJob.class.getCanonicalName();
    private static final long SLEEP_DURATION = 20_000L;

    private static MySqlHelper mySqlHelper;
    private static DataSource dataSource;
    private static JobResolver jobResolver;
    private static TaskRepository taskRepository;

    @BeforeClass
    public static void before() {
        // mySqlHelper = new MySqlHelper(new LocalMySql());
        mySqlHelper = new MySqlHelper(new EmbeddedMySql());
        mySqlHelper.mySqlProfile.start();
        dataSource = mySqlHelper.createDataSource();
        taskRepository = new TaskRepository(dataSource, TABLE_PREFIX);

        jobResolver = new JobResolver();
        jobResolver.register(new SampleLongRunningJob());
        jobResolver.register(new SampleInstantaneousJob());
    }

    @Test
    public void test() throws Exception {
        WorkConfig workConfig = new WorkConfig("host_name_1").logFailures(false).shutDownWait(3);
        // Lock lock = RedisLockProvider.createLock("localhost", 6379);
        SchedulerConfig schedulerConfig = new SchedulerConfig(dataSource, workConfig).withTablePrefix(TABLE_PREFIX).withPollFrequency(2); //.withLock(lock);
        Scheduler scheduler = new Scheduler(schedulerConfig, jobResolver);
        for(int index = 0; index < 2; index++) {
            String key = "index-" + index;
            scheduler.schedulerRegistry().register(new RepeatableTask(NS, key, Duration.apply(300, TimeUnit.SECONDS), JOB_LONG_RUNNING_FQCN).withParameters(key));
        }
        scheduler.schedulerRegistry().register(new RepeatableTask(NS, "Key-9", Duration.apply(300, TimeUnit.SECONDS), JOB_LONG_RUNNING_FQCN).withParameters("Key-9"));
        scheduler.schedulerRegistry().register(new RepeatableTask(NS, "FAIL_IMMEDIATELY", Duration.apply(7, TimeUnit.SECONDS), JOB_LONG_RUNNING_FQCN).withParameters("FAIL_IMMEDIATELY"));
        scheduler.schedulerRegistry().register(new RepeatableTask(NS, "Key-1", Duration.apply(11, TimeUnit.SECONDS), JOB_LONG_RUNNING_FQCN));
        scheduler.schedulerRegistry().register(new RepeatableTask(NS, "Key-2", Duration.apply(7, TimeUnit.SECONDS), JOB_INSTANTANEOUS_FQCN));
        scheduler.schedulerRegistry().register(new CronTask(NS, "Cron-1", "0/6 * * * * ?", JOB_INSTANTANEOUS_FQCN));
        scheduler.start();
        Thread.sleep(SLEEP_DURATION);
        Assert.assertEquals(7, scheduler.jobKeys(NS).size());
        scheduler.stop();
        validateTasks();
    }

    private void validateTasks() {
        Map<String, TaskDetails> map = new HashMap<>();
        Iterator<TaskDetails> iterator = taskRepository.fetchAll().iterator();
        while (iterator.hasNext()) {
            TaskDetails taskDetails = iterator.next();
            map.put(taskDetails.key(), taskDetails);
        }
        Assert.assertEquals(TaskStatus.INTERRUPTED(), map.get("Key-9").status());
        Assert.assertEquals(TaskStatus.INTERRUPTED(), map.get("Key-1").status());
        Assert.assertEquals(3, map.get("Key-2").executions());
        Assert.assertEquals(TaskStatus.SUCCEEDED(), map.get("Key-2").status());
        Assert.assertEquals(3, map.get("FAIL_IMMEDIATELY").executions());
        Assert.assertEquals(TaskStatus.FAILED(), map.get("FAIL_IMMEDIATELY").status());
        Assert.assertEquals(3, map.get("Cron-1").executions());
        Assert.assertEquals(TaskStatus.SUCCEEDED(), map.get("Cron-1").status());
    }

    @AfterClass
    public static void tearDown() {
        mySqlHelper.mySqlProfile.stop();
    }
}
