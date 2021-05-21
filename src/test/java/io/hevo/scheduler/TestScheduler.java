package io.hevo.scheduler;

import io.hevo.scheduler.config.SchedulerConfig;
import io.hevo.scheduler.config.WorkerConfig;
import io.hevo.scheduler.dto.task.RepeatableTask;
import io.hevo.scheduler.helpers.MySqlHelper;
import io.hevo.scheduler.jobs.SimpleJob1;
import io.hevo.scheduler.jobs.SimpleJob2;
import io.hevo.scheduler.helpers.profile.LocalMySql;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

public class TestScheduler {

    private static MySqlHelper mySqlHelper;
    private static DataSource dataSource;
    private static JobResolver jobResolver;

    @BeforeClass
    public static void before() {
        mySqlHelper = new MySqlHelper(new LocalMySql());
        mySqlHelper.mySqlProfile.start();
        dataSource = mySqlHelper.createDataSource();

        jobResolver = new JobResolver();
        jobResolver.register(new SimpleJob1());
        jobResolver.register(new SimpleJob2());
    }

    @Test
    public void test() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig("test.host.com");
        SchedulerConfig schedulerConfig = new SchedulerConfig(dataSource, workerConfig);
        schedulerConfig.tablePrefix("mysql_");
        schedulerConfig.lock_$eq(LocalRedisLockProvider.createLock());
        Scheduler scheduler = new Scheduler(schedulerConfig, jobResolver);
        scheduler.schedulerRegistry().register(new RepeatableTask("NS", "K1", Duration.apply(11, TimeUnit.SECONDS), "io.hevo.scheduler.jobs.SimpleJob1"));
        scheduler.schedulerRegistry().register(new RepeatableTask("NS", "K2", Duration.apply(33, TimeUnit.SECONDS), "io.hevo.scheduler.jobs.SimpleJob2"));
        scheduler.start();
        Thread.sleep(240_000);
        scheduler.stop();
    }

    @AfterClass
    public static void tearDown() {
        mySqlHelper.mySqlProfile.stop();
    }
}
