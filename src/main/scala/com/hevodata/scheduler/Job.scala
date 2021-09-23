package com.hevodata.scheduler

import com.hevodata.scheduler.core.Constants
import com.hevodata.scheduler.dto.ExecutionContext

/**
 * Trait for Job execution
 */
trait Job {
  def execute(context: ExecutionContext): ExecutionStatus.Status

  /**
   * An approximation on what the max run time of this job should be kept as.
   * This number is just for assuming we have lost contact with the executor and are going to mark the task as Expired after this much time has lapsed.
   * The execution should typically be in seconds.
   * (In seconds)
   */
  def maxRunTime(): Long = Constants.MaxRunTime
}

/**
 * Only OBSOLETE is of relevance as of now. If the response returned is OBSOLETE, the job will be removed from the registry
 */
object ExecutionStatus extends Enumeration {
  type Status = Value
  val SUCCEEDED, FAILED, OBSOLETE = Value
}
