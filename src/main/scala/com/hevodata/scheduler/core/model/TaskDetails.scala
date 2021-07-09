package com.hevodata.scheduler.core.model

import java.util.Date

import com.hevodata.scheduler.core.service.SchedulerService
import com.hevodata.scheduler.util.Util
import org.slf4j.LoggerFactory

abstract class TaskDetails(_nameSpace: String, _key: String, _scheduleExpression: String, _handlerClassName: String) {
  private val LOG = LoggerFactory.getLogger(classOf[TaskDetails])
  var id: Long = _
  val namespace: String = _nameSpace
  val key: String = _key
  val scheduleExpression: String = _scheduleExpression

  var executionTime: Date = _
  var nextExecutionTime: Date = new Date

  val handlerClassName: String = _handlerClassName
  var parameters: String = _

  var status: TaskStatus.Value = TaskStatus.INIT

  var executions: Long = _
  var failures: Int = _

  def schedule(): Any

  def discriminator(): TaskType.TaskType

  def primaryParameters(): String = {
    "%s_%s".format(handlerClassName, scheduleExpression)
  }

  /**
   * Calculates the applicable next execution time
   * If the reference date is too near in the future (within 5 seconds from now), delay it by at least then
   */
  def calculateNextExecutionTime(reference: Date): Date = {
    val deltaFromNow: Long = System.currentTimeMillis() + Util.secondsToMillis(SchedulerService.MinJobExecutionGap)
    if(reference.getTime < deltaFromNow) new Date(deltaFromNow) else reference
  }
}
