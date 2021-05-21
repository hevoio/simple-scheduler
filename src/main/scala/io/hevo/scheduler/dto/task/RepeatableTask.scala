package io.hevo.scheduler.dto.task

import java.util.concurrent.TimeUnit

import io.hevo.scheduler.core.Constants

import scala.concurrent.duration.{Duration, FiniteDuration}

case class RepeatableTask(_nameSpace: String = Constants.DefaultNamespace, _key: String, _duration: Duration, _handlerClassName: String) extends Task(_nameSpace, _key, _handlerClassName) {
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