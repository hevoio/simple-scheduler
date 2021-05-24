package io.hevo.scheduler.dto

/**
 * ToDo: Support the parameters as a Map
 * @param parameters required by the Job execution. It can be a JSON serialized parameters map.
 */
case class ExecutionContext(parameters: String) {
  def this() {
    this(null)
  }
}
