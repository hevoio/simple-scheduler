package io.hevo.scheduler.core.service

import java.text.SimpleDateFormat
import java.util.concurrent.locks.{Lock, ReentrantLock}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory, TimeUnit}
import java.util.{Date, Optional}

import io.hevo.scheduler.{ExecutionStatus, Job, Scheduler, SchedulerStatus}
import io.hevo.scheduler.config.WorkConfig
import io.hevo.scheduler.core.Constants
import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.model.{TaskDetails, TaskStatus}
import io.hevo.scheduler.dto.{ExecutionContext, ExecutionResponseData}
import io.hevo.scheduler.handler.JobHandlerFactory
import io.hevo.scheduler.util.Util
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

/**
 * The core of the scheduler
 */
class SchedulerService private(jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, schedulerRegistry: SchedulerRegistry, lockHandler: LockHandler) extends AutoCloseable {

  private val LOG = LoggerFactory.getLogger(classOf[SchedulerService])
  private var workerPool: ExecutorService = _
  private var workConfig: WorkConfig = _

  private val workLock: Lock = new ReentrantLock

  def this(config: WorkConfig, jobHandlerFactory: JobHandlerFactory, taskRepository: TaskRepository, schedulerRegistry: SchedulerRegistry, lockHandler: LockHandler) {
    this(jobHandlerFactory, taskRepository, schedulerRegistry, lockHandler)
    this.workConfig = config

    this.cleanupOnStart()

    val schedulerWorkerThreadFactory: ThreadFactory = new SchedulerThreadFactory("scheduler-worker")
    this.workerPool = Executors.newFixedThreadPool(config.workers, schedulerWorkerThreadFactory)
  }

  /**
   * This is called by main scheduler thread periodically.
   * It is also called as the last step of the work being performed by a worker thread in some cases
   */
  def process(): Boolean = {
    var worked: Boolean = false
    Try {
      val lockAcquired: Boolean = workLock.tryLock()
      if(Scheduler.isActive && lockAcquired) {
        try worked = this.doWork()
        finally {
          if(lockAcquired) {
            workLock.unlock()
          }
        }
      }
    }
    worked
  }

  def doWork(): Boolean = {
    var lockAcquired: Boolean = false
    try {
      lockAcquired = lockHandler.acquire(SchedulerService.GlobalWorkLockId, SchedulerService.LockLife)
      if(lockAcquired) {
        val requestSize: Int = workConfig.tasksToRequest(SchedulerService.UnfinishedTasks.size)
        LOG.debug("Attempting to request: {} tasks", requestSize)
        if(requestSize > 0) {
          val tasks: List[TaskDetails] = taskRepository.fetch(TaskStatus.EXECUTABLE_STATUSES, requestSize, workConfig.maxLookAheadTime)
          // The check on requestSize >= workConfig.workers is required so that fetch attempts don't go on in very short loops
          SchedulerService.HadReceivedPlenty = tasks.size == requestSize && requestSize >= workConfig.workers
          tasks.foreach(task => {
            SchedulerService.UnfinishedTasks.add(task.id)
            this.workerPool.submit(new Handler(task))
          })
          taskRepository.markPicked(tasks.map(_.id), workConfig.appId)
        }
      }
    }
    catch {
      case e: Exception => LOG.error("Failed to find the jobs to process", e)
    }
    finally {
      if(lockAcquired) {
        Util.throwOnError(Try {
          lockHandler.release(SchedulerService.GlobalWorkLockId)
        })
      }
    }
    this.trySyncingProcessedTaskInformation()
    lockAcquired
  }

  def onSuccess(task: TaskDetails, executionStatus: ExecutionStatus.Status): Unit = {
    if(ExecutionStatus.OBSOLETE.equals(executionStatus)) {
      schedulerRegistry.deRegister(task.namespace, task.key)
    }
    else {
      SchedulerService.SuccessTracker.put(task.id, successData(task))
      if (!workConfig.batchUpdates) {
        this.syncProcessedTaskInformationNow()
      }
    }
  }

  def onFailure(task: TaskDetails): Unit = {
    SchedulerService.FailureTracker.put(task.id, failureData(task))
    if(!workConfig.batchUpdates) {
      this.syncProcessedTaskInformationNow()
    }
  }

  def onMissingHandler(task: TaskDetails): Unit = {
    this.schedulerRegistry.deRegister(task.namespace, task.key)
  }

  private def trySyncingProcessedTaskInformation(): Unit = {
    try {
      if (workConfig.batchUpdates && Util.nowWithDelta(-workConfig.batchUpdateFrequency).after(SchedulerService.UpdatesSyncedAt)) {
        this.syncProcessedTaskInformationNow()
      }
    }
    catch {
      case e: Exception => LOG.error("Error while persisting processed task information", e)
    }
  }

  private def syncProcessedTaskInformationNow(): Unit = {
    syncProcessedTaskUpdates(SchedulerService.SuccessTracker, taskRepository.markSucceeded)
    syncProcessedTaskUpdates(SchedulerService.FailureTracker, taskRepository.markFailed)
    SchedulerService.UpdatesSyncedAt = new Date()
  }
  private def syncProcessedTaskUpdates(updatesTracker: mutable.Map[Long, ExecutionResponseData], updater: List[ExecutionResponseData] => Unit): Unit = {
    if(updatesTracker.nonEmpty) {
      val list: List[ExecutionResponseData] = updatesTracker.values.toList
      updater(list)
      list.foreach(update => updatesTracker.remove(update.id))
    }
  }

  private def successData(task: TaskDetails): ExecutionResponseData = {
    val nextExecutionTime: Long = task.calculateNextExecutionTime(referenceTimeForNextExecution(task)).getTime
    ExecutionResponseData(task.id, System.currentTimeMillis(), nextExecutionTime, TaskStatus.SUCCEEDED)
  }

  private def failureData(task: TaskDetails): ExecutionResponseData = {
    val targetStatus: TaskStatus.Status = if (Scheduler.isInterrupted) TaskStatus.INTERRUPTED else TaskStatus.FAILED
    val nextExecutionTime: Long = if (Scheduler.isInterrupted) Util.nowWithDelta(SchedulerService.MinJobExecutionGap).getTime else task.calculateNextExecutionTime(referenceTimeForNextExecution(task)).getTime
    ExecutionResponseData(task.id, System.currentTimeMillis(), nextExecutionTime, targetStatus)
  }

  private def referenceTimeForNextExecution(task: TaskDetails): Date = {
    if(workConfig.nextRelativeToNow) new Date() else task.nextExecutionTime
  }

  /**
   * In some cases, the tasks may stay in the PICKED state in case of an app crash or otherwise.
   * This operation move such tasks to EXPIRED state. Make sure that the cleanupFrequency is larger than the maximum time for which any job can legitimately run
   * Note that if a lock is used, the lock is not released for the entire duration so that other instances of the Scheduler also don't attempt the cleanups
   */
  def attemptCleanup(): Unit = {
    if(Util.nowWithDelta(-workConfig.cleanupFrequency).after(SchedulerService.CleanupAttemptedAt)) {
      try {
        if(lockHandler.acquire(SchedulerService.GlobalCleanupLockId, workConfig.cleanupFrequency)) {
          taskRepository.markExpired(TaskStatus.PICKED, workConfig.cleanupFrequency)
        }
      }
      finally {
        SchedulerService.CleanupAttemptedAt =  new Date()
      }
    }
  }

  /**
   * When the scheduler instance starts, all of the jobs that were "stuck" in the PICKED state are marked as EXPIRED for this scheduler instance, identified by the app_id
   */
  private def cleanupOnStart(): Unit = taskRepository.update(TaskStatus.PICKED, TaskStatus.EXPIRED, this.workConfig.appId)

  override def close(): Unit = {
    Scheduler.Status = SchedulerStatus.STOPPING
    LOG.info("Scheduler is shutting down. Unfinished tasks: {} at {}", SchedulerService.UnfinishedTasks, new SimpleDateFormat("dd-MMM hh:mm:ss:SSS").format(new Date()))
    if(null != this.workerPool && !this.workerPool.isShutdown) {
      this.workerPool.shutdown()
      try  {
        this.workerPool.awaitTermination(Constants.ShutDownWait, TimeUnit.SECONDS)
        this.workerPool.shutdownNow
        Scheduler.Status = SchedulerStatus.INTERRUPTED
      }
      catch {
        case _: InterruptedException =>
          Scheduler.Status = SchedulerStatus.INTERRUPTED
          this.workerPool.shutdownNow
      }
    }
    Try {
      this.syncProcessedTaskInformationNow()
    }
    LOG.info("All workers shut-down. Unfinished tasks: {} at {}", SchedulerService.UnfinishedTasks, new SimpleDateFormat("dd-MMM hh:mm:ss:SSS").format(new Date()))
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
        case e: Exception =>
          if(workConfig.logFailures) {
            LOG.error("Failed to process task: {}", task.id, e)
          }
          onFailure(task)
      }
      finally {
        this.onEventuality()
      }
    }

    private def onEventuality(): Unit = {
      SchedulerService.UnfinishedTasks.remove(task.id)
      if(SchedulerService.UnfinishedTasks.size < workConfig.workers && SchedulerService.HadReceivedPlenty) {
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

  val SuccessTracker: mutable.Map[Long, ExecutionResponseData] = mutable.Map()
  val FailureTracker: mutable.Map[Long, ExecutionResponseData] = mutable.Map()

  /**
   * In seconds
   */
  val MinJobExecutionGap = 5

  var UpdatesSyncedAt: Date = new Date()
  var CleanupAttemptedAt: Date =  new Date()
  var HadReceivedPlenty: Boolean = false
}
