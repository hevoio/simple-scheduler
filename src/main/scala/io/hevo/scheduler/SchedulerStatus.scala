package io.hevo.scheduler

object SchedulerStatus extends Enumeration {
  type Status = Value
  val ACTIVE, STOPPING, INTERRUPTED, STOPPED = Value
}
