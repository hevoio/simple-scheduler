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

  var executions: Long = _
  var failures: Int = _

  def schedule(): Any

  def discriminator(): TaskType.TaskType
  def qualifiedName(): String = TaskDetails.toQualifiedName(namespace, key)

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

object TaskDetails {
  val Separator = "___"
  val QualifiedNameTemplate: String = "%s%s%s"

  def toQualifiedName(namespace: String, key: String): String = {
    QualifiedNameTemplate.format(namespace, Separator, key)
  }

  def fromQualifiedName(qualifiedName: String): (String, String) = {
    val components: Array[String] = qualifiedName.split(Separator, 2)
    (components.apply(0), components.apply(1))
  }

  def namespaceWithSeparator(namespace: String): String = {
    namespace + Separator
  }
}
