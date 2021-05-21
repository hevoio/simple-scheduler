package io.hevo.scheduler.core

object Constants {
  val fieldTableName: String = "table_name"
  val fieldId: String = "id"
  val fieldIds: String = "ids"
  val fieldName: String = "name"
  val fieldParameters: String = "parameters"

  val fieldStatus: String = "status"
  val fieldFromStatus: String = "from_status"
  val fieldToStatus: String = "to_status"

  val fieldType: String = "type"
  val fieldHandlerClass: String = "handler_class"
  val fieldScheduleExpression: String = "schedule_expression"
  val fieldExecutorId: String = "executor_id"

  val fieldExecutionTime: String = "execution_time"
  val fieldNextExecutionTime: String = "next_execution_time"

  val fieldSeconds: String = "seconds"
  val fieldLimit: String = "limit"

  val DefaultNamespace = "DEFAULT"
}
