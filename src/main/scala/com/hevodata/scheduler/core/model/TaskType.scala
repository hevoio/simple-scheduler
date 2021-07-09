package com.hevodata.scheduler.core.model

object TaskType extends Enumeration {
  type TaskType = Value
  val CRON, REPEATABLE = Value
}
