package io.hevo.scheduler

import java.util.Date

import io.hevo.scheduler.core.model.{CronTask, Task}
import org.junit.Test

class SchedulerTest {

  @Test
  def test() {

    val ct1: CronTask = CronTask(1, "A", "B", "sss")
    ct1.executionTime = new Date


    val existing: Map[String, Task] = Map(("ss", ct1))
    val d = existing.get("ss").map(task => task.executionTime).orElse(Option(new Date))
    print(d)

    val d1 = Option(new Date)
    print(d1)

    println(Option(new Date).get)

    val foo: Option[Boolean] = Option.empty

    val ans:Int = foo.map(lock => 5).getOrElse(50)
    println(ans)
  }

}
