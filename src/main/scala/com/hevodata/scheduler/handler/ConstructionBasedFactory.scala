package com.hevodata.scheduler.handler

import java.util.Optional

import com.hevodata.scheduler.Job
import com.hevodata.scheduler.dto.task.Task

/**
 * Default handler class that creates a new instance of the Job class on invocation
 * This requires the class to have a no-arg constructor
 */
class ConstructionBasedFactory extends JobHandlerFactory {

  override def resolve(fqcn: String): Optional[Job] = {
    val clazz: Class[Job] = Task.resolveClass(fqcn)
    val constructor = clazz.getConstructor()
    Optional.of(constructor.newInstance())
  }
}
