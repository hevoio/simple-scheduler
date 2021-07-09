package com.hevodata.scheduler.handler

import java.util.Optional

import com.hevodata.scheduler.Job

trait JobHandlerFactory {
  /**
   * The instance may either be created or retrieved from a registry by the Factory
   * @return An instance of the class whose fully qualified name is @param fqcn
   */
  def resolve(fqcn: String): Optional[Job]
}
