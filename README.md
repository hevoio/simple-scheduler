# Simple Scheduler
A simple to use, lightweight clustered scheduler for Java/Scala. It can be used for job processing also, but it is recommended that the operations are restricted to light-weight processing only.

#### Features
- **Clustered**: When a Lock is used, guarantees execution by a single scheduler instance
- **Requirements**: A single relational database table.
- **Impact on database**: Does not require pessimistic locking. With the possibility to process updates in batches, reduces the write load on the database. The task fetches adapt to the pending work-load

#### Getting Started

**1.** Include the jar as a maven dependency<br/>
**2.** Create the [database table](https://github.com/hevoio/simple-scheduler/blob/master/src/test/resources/db/001.sql).<br/>
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
Lock lock = new RedisBasedLock(String namespace, JedisPool jedisPool)
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

#### Configurations
- [WorkConfig](https://github.com/hevoio/simple-scheduler/blob/master/src/main/scala/io/hevo/scheduler/config/WorkConfig.scala)
- [SchedulerConfig](https://github.com/hevoio/simple-scheduler/blob/master/src/main/scala/io/hevo/scheduler/config/SchedulerConfig.scala)

#### Workflow

![Workflow](https://cdn.hevodata.com/github/simple-scheduler-v2.png)

#### Using in a Java project

<details>
  <summary>Maven Dependencies - Click to expand</summary>
  
  <code>
  
      <dependency>
          <groupId>org.scala-lang</groupId>
          <artifactId>scala-library</artifactId>
          <version>2.11.12</version>
      </dependency>
      <dependency>
          <groupId>com.cronutils</groupId>
          <artifactId>cron-utils</artifactId>
          <version>5.0.5</version>
      </dependency>
      <dependency>
          <groupId>mysql</groupId>
          <artifactId>mysql-connector-java</artifactId>
          <version>8.0.23</version>
      </dependency>
      <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-api</artifactId>
          <version>1.7.5</version>
      </dependency>
      <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-simple</artifactId>
          <version>1.7.5</version>
      </dependency>
      <dependency>
          <groupId>org.apache.commons</groupId>
          <artifactId>commons-dbcp2</artifactId>
          <version>2.8.0</version>
      </dependency>
      
      
      <dependency>
          <groupId>com.hevodata</groupId>
          <artifactId>simple-scheduler_2.11</artifactId>
          <version>0.1.6</version>
      </dependency>
          
  </code>
  
</details>
