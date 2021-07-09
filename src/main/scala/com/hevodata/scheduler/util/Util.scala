package com.hevodata.scheduler.util

import java.util.Date

import scala.util.Random

object Util {

  val Rand: Random = Random

  def getRandom(range: (Int, Int)): Int = {
    range._1 + Rand.nextInt(range._2 - range._1)
  }

  def millisToSeconds(millis: Long): Long = {
    millis / 1000
  }

  def secondsToMillis(seconds: Long): Long = {
    seconds * 1000
  }

  def nowWithDelta(seconds: Long): Date = {
    new Date(System.currentTimeMillis() + secondsToMillis(seconds))
  }
}
