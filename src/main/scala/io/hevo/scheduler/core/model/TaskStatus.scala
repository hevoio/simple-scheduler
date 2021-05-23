package io.hevo.scheduler.core.model

object TaskStatus extends Enumeration {
  type Status = Value
  val INIT, PICKED, SUCCEEDED, INTERRUPTED, EXPIRED, FAILED = Value

  val EXECUTABLE_STATUSES: List[Status] = List(INIT, SUCCEEDED, INTERRUPTED, EXPIRED, FAILED)
}
