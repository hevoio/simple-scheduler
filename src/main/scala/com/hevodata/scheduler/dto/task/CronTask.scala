package com.hevodata.scheduler.dto.task

import com.hevodata.scheduler.core.Constants

import java.time.ZoneId

/**
 * A Schedulable task that executes based on a (Quartz) Cron schedule
 *
 * @param _nameSpace Namespace (Logical grouping) for the task. May be left blank
 * @param _key Unique identifier of a task.
 * @param _cronExpression Cron (Quartz) expression
 * @param _handlerFqcn Fully Qualified Class Name of the handler class
 * parameters: Execution context that would be passed as is to the Handler class
 */
case class CronTask(_nameSpace: String = Constants.DefaultNamespace, _key: String, _cronExpression: String, _timezone: ZoneId, _handlerFqcn: String) extends Task(_nameSpace, _key, _handlerFqcn) {
  val cronExpression: String = _cronExpression
  val timezone: ZoneId = ZoneId.of("UTC")

  def scheduleExpression(): String = {
    _cronExpression
  }
}
