package io.hevo.scheduler.core.model

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.{Date, Optional}

import com.cronutils.mapper.CronMapper
import com.cronutils.model.Cron
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import io.hevo.scheduler.core.Constants

case class CronTaskDetails(_nameSpace: String = Constants.DefaultNamespace, _key: String, _scheduleExpression: String, _handlerClassName: String) extends
  TaskDetails(_nameSpace, _key, _scheduleExpression, _handlerClassName) {

  override def schedule(): Cron = {
    val unixParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(com.cronutils.model.CronType.UNIX))
    CronMapper.fromUnixToQuartz.map(unixParser.parse(scheduleExpression))
  }

  override def discriminator(): TaskType.TaskType = TaskType.CRON

  override def calculateNextExecutionTime(reference: Date): Date = {
    val parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(com.cronutils.model.CronType.UNIX))
    val unixCron = parser.parse(scheduleExpression)

    val duration: Optional[ZonedDateTime] = ExecutionTime.forCron(unixCron).nextExecution(ZonedDateTime.now(ZoneOffset.UTC))
    super.calculateNextExecutionTime(Date.from(duration.get().toInstant))
  }
}
