package com.hevodata.scheduler;

import com.hevodata.scheduler.config.SchedulerConfig;
import com.hevodata.scheduler.config.WorkConfig;
import com.hevodata.scheduler.core.jdbc.TaskRepository;
import com.hevodata.scheduler.core.model.TaskDetails;
import com.hevodata.scheduler.core.model.TaskStatus;
import com.hevodata.scheduler.dto.task.CronTask;
import com.hevodata.scheduler.dto.task.RepeatableTask;
import com.hevodata.scheduler.helpers.MySqlHelper;
import com.hevodata.scheduler.helpers.profile.EmbeddedMySql;
import com.hevodata.scheduler.jobs.SampleInstantaneousJob;
import com.hevodata.scheduler.jobs.SampleLongRunningJob;
import org.junit.*;
import org.junit.runners.MethodSorters;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
    public void test1TaskLifeCycle() throws Exception {
        WorkConfig workConfig = new WorkConfig("host_name_1").logFailures(true).shutDownWait(3);
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

    @Test
    public void test2TaskExpiry() throws Exception {
        WorkConfig workConfig = new WorkConfig("host_name_1").logFailures(true).shutDownWait(3).cleanupFrequency(-2);
        SchedulerConfig schedulerConfig = new SchedulerConfig(dataSource, workConfig).withTablePrefix(TABLE_PREFIX).withPollFrequency(2);
        Scheduler scheduler = new Scheduler(schedulerConfig, jobResolver);
        scheduler.schedulerRegistry().register(new RepeatableTask(NS, "Short-1", Duration.apply(300, TimeUnit.SECONDS), JOB_INSTANTANEOUS_FQCN));
        scheduler.schedulerRegistry().register(new CronTask(NS, "Short-2", "0/6 * * * * ?", JOB_INSTANTANEOUS_FQCN));
        scheduler.schedulerRegistry().register(new CronTask(NS, "Short-3", "0/3 * * * * ?", JOB_INSTANTANEOUS_FQCN));
        scheduler.schedulerRegistry().register(new CronTask(NS, "Long-Running-2", "0/6 * * * * ?", JOB_LONG_RUNNING_FQCN));
        scheduler.schedulerRegistry().register(new CronTask(NS, "Long-Running-3", "0/6 * * * * ?", JOB_LONG_RUNNING_FQCN));

        scala.collection.Map<String, TaskDetails> map = taskRepository.get(NS,
            JavaConversions.asScalaBuffer(Arrays.asList("Short-1", "Short-2", "Short-3", "Long-Running-2", "Long-Running-3")
        ).toList());
        List<Object> ids = new ArrayList<>();
        Iterator<TaskDetails> iterator = map.valuesIterator();
        while (iterator.hasNext()) {
            ids.add(iterator.next().id());
        }
        taskRepository.markPicked(JavaConverters.asScalaBufferConverter(ids).asScala().toList(), "host_name_1");

        preparePickedTask(map.get("Short-1").get().id(), 70);    // Will be marked as expired (limit 60)
        preparePickedTask(map.get("Short-2").get().id(), 100);   // Will be marked as expired (limit 60)
        preparePickedTask(map.get("Short-3").get().id(), 20);    // Will stay in picked (limit 60)
        preparePickedTask(map.get("Long-Running-2").get().id(), 110 * 60);  // Will be marked as expired (limit 100*60)
        preparePickedTask(map.get("Long-Running-3").get().id(), 45 * 60);   // Will stay in picked (limit 100*60)

        scheduler.schedulerService().attemptCleanup();

        map = taskRepository.get(NS,
            JavaConversions.asScalaBuffer(Arrays.asList("Short-1", "Short-2", "Short-3", "Long-Running-2", "Long-Running-3")
        ).toList());

        Assert.assertEquals(0, TaskStatus.EXPIRED().compare(map.get("Short-1").get().status()));
        Assert.assertEquals(0, TaskStatus.EXPIRED().compare(map.get("Short-2").get().status()));
        Assert.assertEquals(0, TaskStatus.PICKED().compare(map.get("Short-3").get().status()));
        Assert.assertEquals(0, TaskStatus.EXPIRED().compare(map.get("Long-Running-2").get().status()));
        Assert.assertEquals(0, TaskStatus.PICKED().compare(map.get("Long-Running-3").get().status()));
        scheduler.stop();
    }

    private void preparePickedTask(long taskId, int pickedInPastSeconds) throws SQLException {
        try(Connection connection = dataSource.getConnection()) {
            String sql = String.format("UPDATE %sscheduled_tasks set status = 'PICKED', picked_at = DATE_ADD(NOW(), INTERVAL -%d SECOND) WHERE id = %d", TABLE_PREFIX, pickedInPastSeconds, taskId);
            try(PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    @AfterClass
    public static void tearDown() {
        mySqlHelper.mySqlProfile.stop();
    }
}
