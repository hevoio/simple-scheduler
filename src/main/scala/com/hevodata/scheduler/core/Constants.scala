package com.hevodata.scheduler.core

object Constants {
  val fieldTableName: String = "table_name"
  val fieldId: String = "id"
  val fieldIds: String = "ids"
  val fieldNamespace: String = "namespace"
  val fieldName: String = "name"
  val fieldParameters: String = "parameters"

  val fieldStatus: String = "status"
  val fieldFromStatus: String = "from_status"
  val fieldToStatus: String = "to_status"

  val fieldType: String = "type"
  val fieldHandlerClass: String = "handler_class"
  val fieldScheduleExpression: String = "schedule_expression"
  val fieldTimeZone: String = "timezone"
  val fieldExecutorId: String = "executor_id"

  val fieldPickedTime: String = "picked_at"
  val fieldExecutionTime: String = "execution_time"
  val fieldNextExecutionTime: String = "next_execution_time"

  val fieldFailures: String = "failure_count"
  val fieldExecutions: String = "executions"

  val fieldSeconds: String = "seconds"
  val fieldLimit: String = "limit"

  val DefaultNamespace = "DEFAULT"

  // In seconds
  val InitialDelay: Long = 5
  val MaxRunTime: Long = 45 * 60
}
