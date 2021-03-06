package com.hevodata.scheduler.core.model

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.Date

import com.cronutils.model.{Cron, CronType}
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import com.hevodata.scheduler.core.Constants
import org.slf4j.LoggerFactory

case class CronTaskDetails(_nameSpace: String = Constants.DefaultNamespace, _key: String, _scheduleExpression: String, _handlerClassName: String) extends
  TaskDetails(_nameSpace, _key, _scheduleExpression, _handlerClassName) {

  private val LOG = LoggerFactory.getLogger(classOf[CronTaskDetails])

  override def schedule(): Cron = {
    val quartzParser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
    quartzParser.parse(scheduleExpression)
  }

  override def discriminator(): TaskType.TaskType = TaskType.CRON

  override def calculateNextExecutionTime(reference: Date): Date = {
    val duration: ZonedDateTime = ExecutionTime.forCron(schedule()).nextExecution(ZonedDateTime.ofInstant(reference.toInstant, ZoneOffset.UTC))
    super.calculateNextExecutionTime(new Date(duration.toInstant.toEpochMilli))
  }
}
