package io.hevo.scheduler.core.service

import java.time.Instant
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.{Date, Optional}

import io.hevo.scheduler.{ExecutionStatus, Job}
import io.hevo.scheduler.config.WorkerConfig
import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.model.{TaskDetails, TaskStatus}
import io.hevo.scheduler.dto.{ExecutionContext, TaskSuccessData}
import io.hevo.scheduler.handler.JobHandlerFactory
import io.hevo.scheduler.util.Util
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

class SchedulerService private(jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, schedulerRegistry: SchedulerRegistry, lockHandler: LockHandler) extends AutoCloseable {

  private val LOG = LoggerFactory.getLogger(classOf[SchedulerService])
  private var workerPool: ExecutorService = _
  private var workerConfig: WorkerConfig = _

  private val workLock: Lock = new ReentrantLock

  def this(config: WorkerConfig, jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, schedulerRegistry: SchedulerRegistry, lockHandler: LockHandler) {
    this(jobHandlerFactory, taskRepository, schedulerRegistry, lockHandler)
    this.workerConfig = config

    this.cleanupOnStart()

    val schedulerWorkerThreadFactory: ThreadFactory = (runnable: Runnable) => new Thread(runnable, "scheduler-worker")
    this.workerPool = Executors.newFixedThreadPool(config.workers, schedulerWorkerThreadFactory)
  }

  def process(): Unit = {
    if(workLock.tryLock()) {
      try this.work()
      finally {
        workLock.unlock()
      }
    }
  }

  def work(): Unit = {
    var lockAcquired: Boolean = false
    try {
      lockAcquired = lockHandler.acquire(SchedulerService.GlobalWorkLockId, SchedulerService.LockLife)
      if(lockAcquired) {
        val requestSize: Int = tasksToFetch()
        if(requestSize > 0) {
          val tasks: List[TaskDetails] = taskRepository.fetch(TaskStatus.READY_STATUSES, requestSize, workerConfig.maxLookAheadTime)
          SchedulerService.HadReceivedPlenty = tasks.size == requestSize
          tasks.foreach(task => {
            SchedulerService.UnfinishedTasks.add(task.id)
            this.workerPool.submit(new Handler(task))
          })
          taskRepository.markPicked(tasks.map(_.id), workerConfig.appId)
        }
      }
    }
    catch {
      case e: Exception => LOG.error("Failed to find the jobs to process", e)
    }
    finally {
      if(lockAcquired) {
        Try {
          lockHandler.release(SchedulerService.GlobalWorkLockId)
        }
      }
    }
    this.trySyncingProcessedTaskInformation()
  }

  def onSuccess(task: TaskDetails, executionStatus: ExecutionStatus.Status): Unit = {
    if(ExecutionStatus.OBSOLETE.equals(executionStatus)) {
      schedulerRegistry.deRegister(task.namespace, task.key)
    }
    else {
      SchedulerService.SuccessTracker.put(task.id, successData(task))
      if (!workerConfig.batchUpdates) {
        this.syncProcessedTaskInformationNow()
      }
    }
  }

  def onFailure(task: TaskDetails): Unit = {
    SchedulerService.FailureTracker.put(task.id, new Date)
    if(!workerConfig.batchUpdates) {
      this.syncProcessedTaskInformationNow()
    }
  }

  def onMissingHandler(task: TaskDetails): Unit = {
    this.schedulerRegistry.deRegister(task.namespace, task.key)
  }

  private def trySyncingProcessedTaskInformation(): Unit = {
    try {
      if (workerConfig.batchUpdates && Util.nowWithDelta(-workerConfig.batchUpdateFrequency).after(SchedulerService.UpdatesSyncedAt)) {
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
    SchedulerService.UpdatesSyncedAt =  new Date()
  }

  private def tasksToFetch(): Int = {
    workerConfig.workers * workerConfig.pickupFactor - SchedulerService.UnfinishedTasks.size
  }

  private def successData(task: TaskDetails): TaskSuccessData = {
    val nextExecutionTime: Long = task.calculateNextExecutionTime(task.nextExecutionTime).getTime
    TaskSuccessData(task.id, Instant.now().toEpochMilli, nextExecutionTime)
  }

  def attemptCleanup(): Unit = {
    if(Util.nowWithDelta(-workerConfig.cleanupFrequency).after(SchedulerService.CleanupAttemptedAt)) {
      try {
        if(lockHandler.acquire(SchedulerService.GlobalCleanupLockId, workerConfig.cleanupFrequency)) {
          taskRepository.markExpired(TaskStatus.PICKED, workerConfig.cleanupFrequency)
        }
      }
      finally {
        SchedulerService.CleanupAttemptedAt =  new Date()
      }
    }
  }

  private def cleanupOnStart(): Unit = taskRepository.update(TaskStatus.PICKED, TaskStatus.EXPIRED, this.workerConfig.appId)

  override def close(): Unit = {
    if(null != this.workerPool && !this.workerPool.isShutdown) {
      this.workerPool.shutdown()
      try this.workerPool.awaitTermination(Long.MaxValue, TimeUnit.MILLISECONDS)
      catch {
        case _: InterruptedException => this.workerPool.shutdownNow
      }
    }
  }

  class Handler(task: TaskDetails) extends Runnable {
    override def run(): Unit = {
      try {
        val optionalJobHandler: Optional[Job] = jobHandlerFactory.resolve(task.handlerClassName)
        if(optionalJobHandler.isPresent) {
          val executionStatus: ExecutionStatus.Status = optionalJobHandler.get().execute(ExecutionContext(task.parameters))
          onSuccess(task, executionStatus)
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
        this.onEventuality()
      }
    }

    private def onEventuality(): Unit = {
      SchedulerService.UnfinishedTasks.remove(task.id)
      if(SchedulerService.UnfinishedTasks.size < workerConfig.workers && SchedulerService.HadReceivedPlenty) {
        process()
      }
    }
  }

}

object SchedulerService {
  val UnfinishedTasks: mutable.Set[Long] = mutable.Set()
  val GlobalWorkLockId: String = "scheduler:WORK_ACQUISITION_LOCK"
  val GlobalCleanupLockId: String = "scheduler:CLEANUP_LOCK"
  val LockLife: Int = 60

  val SuccessTracker: mutable.Map[Long, TaskSuccessData] = mutable.Map()
  val FailureTracker: mutable.Map[Long, Date] = mutable.Map()

  /**
   * In seconds
   */
  val MinJobExecutionGap = 5

  var UpdatesSyncedAt: Date = new Date()
  var CleanupAttemptedAt: Date =  new Date()
  var HadReceivedPlenty: Boolean = false
}
