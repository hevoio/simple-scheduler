# Simple Scheduler
A simple to use, lightweight clustered scheduler for Java/Scala. It can be used for job processing also, but it is recommended that the operations are restricted to light-weight processing only.

### Features
- **Clustered**: When a Lock is used, guarantees execution by a single scheduler instance
- **Requirements**: A single relational database table.
- **Impact on database**: Does not require pessimistic locking. With the possibility to process updates in batches, reduces the write load on the database. The task fetches adapt to the pending work-load

### Getting Started

**1.** Include the jar as a maven dependency<br/>
**2.** Create the [database table](https://github.com/hevoio/simple-scheduler/blob/HEVO-4395/src/test/resources/db/001.sql).<br/>
**3.** Create a DataSource reference. eg: 
``` BasicDataSource dataSource = new BasicDataSource();
dataSource.setDriverClassName("com.mysql.jdbc.Driver");
dataSource.setUsername(mySqlProfile.user);
dataSource.setPassword(mySqlProfile.password);
dataSource.setUrl("jdbc:mysql://hostname:3306/my_db_schema");
```
**4.** Create a JobResolverFactory or use the default job resolver that creates a new instance of the executor class on each invocation
```
JobResolverFactory jobResolverFactory = new ConstructionBasedFactory();
```

**5.** (Not required in a single instance mode) Create a Lock or use the default Redis based Lock

```
Lock lock = new RedisBasedLock(JedisPool jedisPool)
```

**6.** Create the Scheduler instance and start it
```
WorkConfig workConfig = new WorkConfig("host_name_1").logFailures(false);
SchedulerConfig schedulerConfig = new SchedulerConfig(dataSource, workConfig).withLock(lock);
Scheduler scheduler = new Scheduler(schedulerConfig, jobResolverFactory);
scheduler.start()
```
**7.** Register a few tasks (one time) irrespective of JVM restarts
```
scheduler.schedulerRegistry().register(new RepeatableTask("Fizz_Namespace", "PollJob-9", Duration.apply(30, TimeUnit.SECONDS), "com.foo.bar.poll.ReadJob").withParameters("Xing"));
scheduler.schedulerRegistry().register(new CronTask("Buzz_Namespace", "SR-192", "0/33 * * * * ?", "com.foo.bar.poll.SpecificReadsJob"));
```
**8.** Stop the Scheduler
```
scheduler.stop()
```

### Configurations
- [WorkConfig](https://github.com/hevoio/simple-scheduler/blob/HEVO-4395/src/main/scala/io/hevo/scheduler/config/WorkConfig.scala)
- [SchedulerConfig](https://github.com/hevoio/simple-scheduler/blob/HEVO-4395/src/main/scala/io/hevo/scheduler/config/SchedulerConfig.scala)