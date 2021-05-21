package io.hevo.scheduler.dto.task

import io.hevo.scheduler.core.Constants

case class CronTask(_nameSpace: String = Constants.DefaultNamespace, _key: String, _cronExpression: String, _handlerClassName: String) extends Task(_nameSpace, _key, _handlerClassName) {
  val cronExpression: String = _cronExpression

  def scheduleExpression(): String = {
    _cronExpression
  }
}
