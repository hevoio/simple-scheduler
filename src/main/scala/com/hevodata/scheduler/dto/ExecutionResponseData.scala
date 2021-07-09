package com.hevodata.scheduler.dto

import com.hevodata.scheduler.core.model.TaskStatus.Status

case class ExecutionResponseData(id: Long, actualExecutionTime: Long, nextExecutionTime: Long, targetStatus: Status) { }
