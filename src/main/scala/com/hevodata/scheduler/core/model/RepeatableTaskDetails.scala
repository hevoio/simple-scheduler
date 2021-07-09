package com.hevodata.scheduler.core.model

import java.util.Date

import com.hevodata.scheduler.core.Constants
import com.hevodata.scheduler.dto.task.RepeatableTask

import scala.concurrent.duration.Duration

case class RepeatableTaskDetails(_nameSpace: String = Constants.DefaultNamespace, _key: String, _scheduleExpression: String, _handlerClassName: String) extends
  TaskDetails(_nameSpace, _key, _scheduleExpression, _handlerClassName) {

  override def schedule(): Duration = RepeatableTask.toDuration(this.scheduleExpression)

  override def discriminator(): TaskType.TaskType = TaskType.REPEATABLE

  override def calculateNextExecutionTime(reference: Date): Date = {
    super.calculateNextExecutionTime(new Date(reference.getTime + schedule().toMillis))
  }
}
