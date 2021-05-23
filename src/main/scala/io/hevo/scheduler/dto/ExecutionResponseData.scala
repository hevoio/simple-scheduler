package io.hevo.scheduler.dto

import io.hevo.scheduler.core.model.TaskStatus.Status

case class ExecutionResponseData(id: Long, actualExecutionTime: Long, nextExecutionTime: Long, targetStatus: Status) { }
