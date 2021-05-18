package io.hevo.scheduler.handler

import java.util.Optional

import io.hevo.scheduler.Job

trait JobHandlerFactory {
  /**
   * @return An instance of the class whose fully qualified name is @param fqcn
   */
  def resolve(fqcn: String): Optional[Job]
}
