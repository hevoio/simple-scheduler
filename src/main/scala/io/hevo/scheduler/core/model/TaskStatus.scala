package io.hevo.scheduler.core.model

import scala.jdk.javaapi.CollectionConverters

object TaskStatus extends Enumeration {
  type Status = Value
  val INIT, PICKED, SUCCEEDED, EXPIRED, BAD_CONFIG, FAILED = Value

  val ACTIVE_STATUSES: java.util.List[Status] = CollectionConverters.asJava(List(INIT, PICKED, SUCCEEDED, EXPIRED, BAD_CONFIG))
}
