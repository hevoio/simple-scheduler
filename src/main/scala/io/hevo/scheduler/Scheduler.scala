package io.hevo.scheduler

import java.util.Date
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import io.hevo.scheduler.config.SchedulerConfig
import io.hevo.scheduler.core.Constants
import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.service.{LockHandler, SchedulerRegistry, SchedulerService}
import io.hevo.scheduler.dto.task.Task
import io.hevo.scheduler.handler.{ConstructionBasedFactory, JobHandlerFactory}
import org.slf4j.LoggerFactory

class Scheduler private {

  private val LOG = LoggerFactory.getLogger(classOf[Scheduler])

  private var config: SchedulerConfig = _
  private var monitor: ScheduledExecutorService = _

  private var schedulerService: SchedulerService = _
  var schedulerRegistry: SchedulerRegistry = _

  def this(config: SchedulerConfig, jobHandlerFactory: JobHandlerFactory = new ConstructionBasedFactory) {
    this
    this.config = config
    val taskRepository: TaskRepository = new TaskRepository(config.dataSource, config.tablePrefix)
    this.schedulerRegistry = new SchedulerRegistry(taskRepository)
    this.schedulerService = new SchedulerService(config.workerConfig, jobHandlerFactory, taskRepository, this.schedulerRegistry, new LockHandler(Option(config.lock)))
  }

  /**
   * Starts the Scheduler.
   * The scheduler need not be started to register/de-register the tasks
   */
  def start(): Unit = {
    val schedulerMonitorThreadFactory: ThreadFactory = (runnable: Runnable) => new Thread(runnable, "scheduler-monitor")
    this.monitor = Executors.newSingleThreadScheduledExecutor(schedulerMonitorThreadFactory)
    this.monitor.scheduleAtFixedRate(runner, Constants.InitialDelay, config.pollFrequency, TimeUnit.SECONDS)
    Scheduler.Status = SchedulerStatus.ACTIVE
  }

  /**
   * Stops the Scheduler and releases the resources
   */
  def stop(): Unit = {
    try this.schedulerService.close()
    catch {
      case e: Exception => LOG.error("Failed to stop the Scheduler Service", e)
    }

    if(null != this.monitor) {
      this.monitor.shutdown()
      try this.monitor.awaitTermination(Constants.ShutDownWait, TimeUnit.SECONDS)
      catch {
        case _: InterruptedException => this.monitor.shutdownNow
      }
    }
    Scheduler.Status = SchedulerStatus.STOPPED
  }

  /**
   * Registers a task. If it is a new task, it is scheduled to be run immediately
   * If the task is already registered, it is not re-registered but some of the attributes like the handler class and the schedule expression may be updated
   * The next execution schedule is adjusted based on the new schedule expression.
   * If the task is currently executing, the revised schedule is applied to the next run instance
   */
  def register(task: Task): Unit = {
    this.schedulerRegistry.register(task)
  }

  /**
   * Register a set of tasks
   */
  def register(tasks: List[Task]): Unit = {
    this.schedulerRegistry.register(tasks)
  }

  /**
   * De-register a task identified by the namespace and the key
   */
  def deRegister(namespace: String, key: String): Unit = {
    this.schedulerRegistry.deRegister(namespace, key)
  }
  /**
   * Find the next execution time of a task identified by the namespace and the key
   */
  def nextExecutionTime(namespace: String, key: String): Date = {
    this.schedulerRegistry.nextExecutionTime(namespace, key)
  }

  private val runner = new Runnable {
    def run(): Unit = {
      try {
        val worked: Boolean = schedulerService.process()
        LOG.debug("Scheduler instance was able to work: {}", worked)
        schedulerService.attemptCleanup()
      }
      catch {
        case e: Exception => LOG.error("Failed to process the scheduler job", e)
      }
    }
  }
}

object Scheduler {
  @volatile var Status: SchedulerStatus.Status = SchedulerStatus.STOPPED

  def isActive: Boolean = {
    SchedulerStatus.ACTIVE == Status
  }

  def isInterrupted: Boolean = {
    SchedulerStatus.INTERRUPTED == Status
  }
}