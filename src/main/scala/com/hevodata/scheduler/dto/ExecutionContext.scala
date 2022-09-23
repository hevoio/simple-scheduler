package com.hevodata.scheduler.dto

import java.util.Date

/**
 * ToDo: Support the parameters as a Map
 *
 * @param parameters required by the Job execution. It can be a JSON serialized parameters map.
 */
case class ExecutionContext(parameters: String, nextExecutionTime: Date) {
  def this() {
    this(null, null)
  }
}
