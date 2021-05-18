package io.hevo.scheduler.core.model

import java.time.Instant
import java.util.Date

import io.hevo.scheduler.Job
import io.hevo.scheduler.core.exception.HandlerException
import io.hevo.scheduler.core.service.SchedulerService

abstract class Task(_id: Long, _nameSpace: String, _key: String, _scheduleExpression: String) {

  val id: Long = _id
  val namespace: String = _nameSpace
  val key: String = _key
  val scheduleExpression: String = _scheduleExpression

  var executionTime: Date = _
  var nextExecutionTime: Date = new Date

  var handlerClassName: String = _
  var parameters: Map[String, _] = _

  var status: TaskStatus.Value = TaskStatus.INIT

  def schedule(): Any

  def discriminator(): TaskType.TaskType
  def resolveClass(): Class[Job] = Task.resolveClass(handlerClassName)
  def qualifiedName(): String = Task.toQualifiedName(namespace, key)

  /**
   * Calculates the applicable next execution time
   * If the reference date is too near in the future (within 5 seconds from now), delay it by at least then
   */
  def calculateNextExecutionTime(reference: Date): Date = {
    val deltaFromNow: Instant = Instant.now().plusSeconds(SchedulerService.MinJobExecutionGap)
    if(reference.getTime < deltaFromNow.toEpochMilli) Date.from(deltaFromNow) else reference
  }
}

object Task {
  val qualifiedNameTemplate: String = "%s_%s"

  def toQualifiedName(namespace: String, key: String): String = {
    qualifiedNameTemplate.format(namespace, key)
  }

  def fromQualifiedName(qualifiedName: String): (String, String) = {
    val components: Array[String] = qualifiedName.split("_")
    (components.apply(0), components.apply(1))
  }

  def resolveClass(fqcn: String): Class[Job] = {
    val clazz: Class[_] = Class.forName(fqcn)
    if(!classOf[Job].isAssignableFrom(clazz)) {
      throw new HandlerException("Handler class is not an instance of Job")
    }
    clazz.asInstanceOf[Class[Job]]
  }
}
