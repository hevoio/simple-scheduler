package io.hevo.scheduler.core.service

import java.util.Date

import io.hevo.scheduler.core.jdbc.TaskRepository
import io.hevo.scheduler.core.model.Task

class SchedulerRegistry(taskRepository: TaskRepository) {

  def register(task: Task): Unit = {
    this.register(List(task))
  }

  def register(tasks: List[Task]): Unit = {
    validate(tasks)
    val request: Map[String, List[Task]] = tasks.groupBy(task => task.namespace)
    for((namespace, list) <- request) {
      val existing: Map[String, Task] = taskRepository.get(namespace, list.map(task => task.key))
      list.foreach(task => task.nextExecutionTime = task.calculateNextExecutionTime(existing.get(task.key).map(task => task.executionTime).orElse(Option(new Date)).get))
      taskRepository.add(list)
    }
  }
  private def validate(tasks: List[Task]): Unit = {
    tasks.foreach(task => task.resolveClass())
  }

  def deRegister(namespace: String, key: String): Unit = {
    taskRepository.delete(namespace, List(key))
  }

  def nextExecutionTime(namespace: String, key: String): Date = {
    val data: Map[String, Task] = taskRepository.get(namespace, List(key))
    data.get(key).map(record => record.nextExecutionTime).orElse(Option(new Date)).get
  }
}
