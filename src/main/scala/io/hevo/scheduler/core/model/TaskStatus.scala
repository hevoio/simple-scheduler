package io.hevo.scheduler.core.model

object TaskStatus extends Enumeration {
  type Status = Value
  val INIT, PICKED, SUCCEEDED, EXPIRED, FAILED = Value

  val READY_STATUSES: List[Status] = List(INIT, SUCCEEDED, EXPIRED)
}
