package io.hevo.scheduler.dto.task

import java.util.concurrent.TimeUnit

import io.hevo.scheduler.core.Constants

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * A Schedulable task that needs to be repeated at a fixed frequency.
 * The next run execution would be attempted @param _duration after the previously determined execution time irrespective of when the task actually ran.
 * In case, the next execution is very close to the end of current execution, the next execution is slightly delayed
 *
 * @param _nameSpace Namespace (Logical grouping) for the task. May be left blank
 * @param _key Unique identifier of a task.
 * @param _duration Frequency to repeat at
 * @param _handlerFqcn Fully Qualified Class Name of the handler class
 * parameters: Execution context that would be passed as is to the Handler class
 */
case class RepeatableTask(_nameSpace: String = Constants.DefaultNamespace, _key: String, _duration: Duration, _handlerFqcn: String) extends Task(_nameSpace, _key, _handlerFqcn) {
  val duration: Duration = _duration

  def scheduleExpression(): String = {
    RepeatableTask.toExpression(_duration)
  }
}

object RepeatableTask {

  def toDuration(expression: String): Duration = {
    val components: Array[String] = expression.split(Separator)
    new FiniteDuration(components.apply(1).toLong, TimeUnit.valueOf(components.apply(0)))
  }

  def toExpression(duration: Duration): String = {
    "%s%s%d".format(TimeUnit.SECONDS.name(), Separator, duration.toSeconds)
  }

  val Separator: String = ":"
}