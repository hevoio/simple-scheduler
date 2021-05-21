package io.hevo.scheduler.core.model

import java.util.Date

import io.hevo.scheduler.core.service.SchedulerService
import io.hevo.scheduler.util.Util

abstract class TaskDetails(_nameSpace: String, _key: String, _scheduleExpression: String, _handlerClassName: String) {

  var id: Long = _
  val namespace: String = _nameSpace
  val key: String = _key
  val scheduleExpression: String = _scheduleExpression

  var executionTime: Date = _
  var nextExecutionTime: Date = new Date

  val handlerClassName: String = _handlerClassName
  var parameters: String = _

  var status: TaskStatus.Value = TaskStatus.INIT

  def schedule(): Any

  def discriminator(): TaskType.TaskType
  def qualifiedName(): String = TaskDetails.toQualifiedName(namespace, key)

  /**
   * @return the combination of parameters that define the uniqueness of the task
   */
  def uniqueness(): String = {
    "%s_%s_%s_%s".format(namespace, key, handlerClassName, scheduleExpression)
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

object TaskDetails {
  val qualifiedNameTemplate: String = "%s_%s"

  def toQualifiedName(namespace: String, key: String): String = {
    qualifiedNameTemplate.format(namespace, key)
  }

  def fromQualifiedName(qualifiedName: String): (String, String) = {
    val components: Array[String] = qualifiedName.split("_")
    (components.apply(0), components.apply(1))
  }
}
