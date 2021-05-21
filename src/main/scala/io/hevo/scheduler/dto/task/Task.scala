package io.hevo.scheduler.dto.task

import io.hevo.scheduler.Job
import io.hevo.scheduler.core.exception.HandlerException

abstract class Task(_nameSpace: String, _key: String, _handlerClassName: String) {
  val namespace: String = _nameSpace
  val key: String = _key
  val handlerClassName: String = _handlerClassName
  var parameters: String = _

  def withParameters(parameters: String): Task = {
    this.parameters = parameters
    this
  }
  def resolveClass(): Class[Job] = Task.resolveClass(_handlerClassName)
  def scheduleExpression(): String
}

object Task {
  def resolveClass(fqcn: String): Class[Job] = {
    val clazz: Class[_] = Class.forName(fqcn)
    if(!classOf[Job].isAssignableFrom(clazz)) {
      throw new HandlerException("Handler class is not an instance of Job")
    }
    clazz.asInstanceOf[Class[Job]]
  }
}