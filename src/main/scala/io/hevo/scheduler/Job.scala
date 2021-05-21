package io.hevo.scheduler

import io.hevo.scheduler.dto.ExecutionContext

trait Job {
  def execute(context: ExecutionContext): ExecutionStatus.Status
}

/**
 * Only OBSOLETE is of relevance as of now. If the response returned is OBSOLETE, the job will be removed from the registry
 */
object ExecutionStatus extends Enumeration {
  type Status = Value
  val SUCCEEDED, FAILED, OBSOLETE = Value
}
