package io.hevo.scheduler

import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}

import io.hevo.scheduler.config.SchedulerConfig
import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.service.{LockHandler, SchedulerRegistry, SchedulerService}
import io.hevo.scheduler.handler.{ConstructionBasedFactory, JobHandlerFactory}
import org.slf4j.LoggerFactory

class Scheduler private {

  private val LOG = LoggerFactory.getLogger(classOf[Scheduler])

  private val config: SchedulerConfig = null
  private var monitor: ScheduledExecutorService = _

  private var schedulerService: SchedulerService = _
  var schedulerRegistry: SchedulerRegistry = _

  def this(app_id: String, config: SchedulerConfig, jobHandlerFactory: JobHandlerFactory = new ConstructionBasedFactory) {
    this
    val taskRepository: TaskRepository = new TaskRepository(config.orm)
    this.schedulerService = new SchedulerService(config.workerConfig, jobHandlerFactory, taskRepository, new LockHandler(Option(config.lock)))
    this.schedulerRegistry = new SchedulerRegistry(taskRepository)
  }

  def start(): Unit = {
    val schedulerMonitorThreadFactory: ThreadFactory = (runnable: Runnable) => new Thread(runnable, "scheduler-monitor")
    this.monitor = Executors.newSingleThreadScheduledExecutor(schedulerMonitorThreadFactory)
    this.monitor.scheduleAtFixedRate(runner, config.pollFrequency, config.pollFrequency, TimeUnit.SECONDS)
  }

  private val runner = new Runnable {
    def run(): Unit = {
      try schedulerService.process()
      catch {
        case e: Exception => LOG.error("Failed to process the scheduler job", e)
      }
    }
  }

  def stop(): Unit = {
    try this.schedulerService.close()
    catch {
      case e: Exception => LOG.error("Failed to stop the Scheduler Service", e)
    }

    if(null != this.monitor) {
      this.monitor.shutdown()
      try this.monitor.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException => this.monitor.shutdownNow
      }
    }
  }
}
