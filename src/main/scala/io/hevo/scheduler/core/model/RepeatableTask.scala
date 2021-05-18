package io.hevo.scheduler.core.model


import java.time.Instant
import java.util.Date
import java.util.concurrent.TimeUnit

import io.hevo.scheduler.core.Constants

import scala.concurrent.duration.{Duration, FiniteDuration}

case class RepeatableTask(_id: Long, _nameSpace: String = Constants.DefaultNamespace, _key: String, _scheduleExpression: String) extends Task(_id, _nameSpace, _key, _scheduleExpression) {

  override def schedule(): Duration = FiniteDuration.apply(this.scheduleExpression.toLong, TimeUnit.SECONDS)

  override def discriminator(): TaskType.TaskType = TaskType.REPEATABLE

  override def calculateNextExecutionTime(reference: Date): Date = {
    super.calculateNextExecutionTime(new Date(Instant.ofEpochMilli(reference.getTime).plusMillis(schedule().toMillis).toEpochMilli))
  }
}
