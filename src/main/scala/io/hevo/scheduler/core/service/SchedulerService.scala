package io.hevo.scheduler.core.service

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.{Date, Optional}

import io.hevo.scheduler.Job
import io.hevo.scheduler.config.WorkerConfig
import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.model.{Task, TaskStatus}
import io.hevo.scheduler.dto.{ExecutionContext, TaskSuccessData}
import io.hevo.scheduler.handler.JobHandlerFactory
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

class SchedulerService private(jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, lockHandler: LockHandler) extends AutoCloseable {

  private val LOG = LoggerFactory.getLogger(classOf[SchedulerService])
  private var workerPool: ExecutorService = _
  private var workerConfig: WorkerConfig = _

  def this(config: WorkerConfig, jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, lockHandler: LockHandler) {
    this(jobHandlerFactory, taskRepository, lockHandler)
    this.workerConfig = config

    val schedulerWorkerThreadFactory: ThreadFactory = (runnable: Runnable) => new Thread(runnable, "scheduler-worker")
    this.workerPool = Executors.newFixedThreadPool(config.workers, schedulerWorkerThreadFactory)
  }

  def process(): Unit = {
    var lockAcquired: Boolean = false
    try {
      lockAcquired = lockHandler.acquire(SchedulerService.LockId, SchedulerService.LockLife)
      if(lockAcquired) {
        val toFetch: Int = tasksToFetch()
        if(toFetch > 0) {
          val tasks: List[Task] = taskRepository.fetch(TaskStatus.ACTIVE_STATUSES, toFetch, workerConfig.maxLookAheadTime)
          tasks.foreach(task => {
            SchedulerService.UnfinishedTasks.add(task.id)
            this.workerPool.submit(new Handler(task))
          })
        }
      }
    }
    catch {
      case e: Exception => LOG.error("Failed to find the jobs to process", e)
    }
    finally {
      if(lockAcquired) {
        Try {
          lockHandler.release(SchedulerService.LockId)
        }
      }
    }
    this.trySyncingProcessedTaskInformation()
  }

  def onSuccess(task: Task): Unit = {
    SchedulerService.SuccessTracker.put(task.id, successData(task))
    if(!workerConfig.batchUpdates) {
      this.syncProcessedTaskInformationNow()
    }
  }

  def onFailure(task: Task): Unit = {
    SchedulerService.FailureTracker.put(task.id, new Date)
    if(!workerConfig.batchUpdates) {
      this.syncProcessedTaskInformationNow()
    }
  }

  def onMissingHandler(task: Task): Unit = {
    SchedulerService.MissingJobTracker.put(task.id, new Date)
    if(!workerConfig.batchUpdates) {
      this.syncProcessedTaskInformationNow()
    }
  }

  private def trySyncingProcessedTaskInformation(): Unit = {
    try {
      if (workerConfig.batchUpdates && Instant.now().minus(workerConfig.batchUpdateFrequency, ChronoUnit.SECONDS).isAfter(SchedulerService.UpdatesSyncedAt)) {
        this.syncProcessedTaskInformationNow()
      }
    }
    catch {
      case e: Exception => LOG.error("Error while persisting processed task information", e)
    }
  }
  private def syncProcessedTaskInformationNow(): Unit = {
    if(SchedulerService.SuccessTracker.nonEmpty) {
      val successDataList: List[TaskSuccessData] = SchedulerService.SuccessTracker.values.toList
      taskRepository.markSucceeded(successDataList)
      successDataList.foreach(successData => SchedulerService.FailureTracker.remove(successData.id))
    }
    if(SchedulerService.FailureTracker.nonEmpty) {
      val ids: List[Long] = SchedulerService.FailureTracker.keys.toList
      taskRepository.markFailed(ids, TaskStatus.FAILED)
      ids.foreach(id => SchedulerService.FailureTracker.remove(id))
    }
    if(SchedulerService.MissingJobTracker.nonEmpty) {
      val ids: List[Long] = SchedulerService.FailureTracker.keys.toList
      taskRepository.markFailed(ids, TaskStatus.BAD_CONFIG)
      ids.foreach(id => SchedulerService.MissingJobTracker.remove(id))
    }
    SchedulerService.UpdatesSyncedAt = Instant.now()
  }

  private def tasksToFetch(): Int = {
    workerConfig.workers * workerConfig.pickupFactor - SchedulerService.UnfinishedTasks.size
  }

  private def successData(task: Task): TaskSuccessData = {
    val nextExecutionTime: Long = task.calculateNextExecutionTime(task.nextExecutionTime).getTime
    TaskSuccessData(task.id, Instant.now().toEpochMilli, nextExecutionTime)
  }

  override def close(): Unit = {
    if(null != this.workerPool && !this.workerPool.isShutdown) {
      this.workerPool.shutdown()
      try this.workerPool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException => this.workerPool.shutdownNow
      }
    }
  }

  class Handler(task: Task) extends Runnable {
    override def run(): Unit = {
      try {
        val optionalJobHandler: Optional[Job] = jobHandlerFactory.resolve(task.handlerClassName)
        if(optionalJobHandler.isPresent) {
          optionalJobHandler.get().execute(ExecutionContext(task.parameters))
          onSuccess(task)
        }
        else {
          onMissingHandler(task)
        }
      }
      catch {
        case e: Exception => LOG.error("Failed to process task: {}", task.id, e)
          onFailure(task)
      }
      finally {
        SchedulerService.UnfinishedTasks.remove(task.id)
      }
    }
  }

}

object SchedulerService {
  val UnfinishedTasks: mutable.Set[Long] = mutable.Set()
  val LockId: String = "scheduler:WORK_ACQUISITION_LOCK"
  val LockLife: Int = 20

  val SuccessTracker: mutable.Map[Long, TaskSuccessData] = mutable.Map()
  val FailureTracker: mutable.Map[Long, Date] = mutable.Map()
  val MissingJobTracker: mutable.Map[Long, Date] = mutable.Map()

  /**
   * In seconds
   */
  val MinJobExecutionGap = 5

  var UpdatesSyncedAt: Instant = Instant.now()
}
