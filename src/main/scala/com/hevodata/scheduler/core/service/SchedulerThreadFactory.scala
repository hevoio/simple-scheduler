package com.hevodata.scheduler.core.service

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

class SchedulerThreadFactory(val prefix: String) extends ThreadFactory {
  override def newThread(runnable: Runnable) = new Thread(runnable, "%s-%d".format(prefix, SchedulerThreadFactory.Counter.incrementAndGet()))
}

object SchedulerThreadFactory {
  val Counter: AtomicInteger = new AtomicInteger()
}