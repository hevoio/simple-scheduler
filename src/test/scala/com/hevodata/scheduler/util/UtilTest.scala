package com.hevodata.scheduler.util

import org.junit.{Assert, Test}

class UtilTest {

  @Test
  def testJobName(): Unit = {
    val jobName = Util.getJobName("io.hevo.connectors.jobs.ConnectorExecutionJob")
    Assert.assertEquals("ConnectorExecutionJob", jobName)
  }
}
