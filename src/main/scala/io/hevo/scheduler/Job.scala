package io.hevo.scheduler

import io.hevo.scheduler.dto.ExecutionContext

trait Job {
  def execute(context: ExecutionContext): Unit
}
