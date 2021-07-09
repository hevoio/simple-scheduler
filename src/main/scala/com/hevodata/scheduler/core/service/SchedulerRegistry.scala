package com.hevodata.scheduler.core.service

import java.util.Date

import com.hevodata.scheduler.core.jdbc.{TaskMapper, TaskRepository}
import com.hevodata.scheduler.core.model.TaskDetails
import com.hevodata.scheduler.dto.task.Task
import com.hevodata.scheduler.core.jdbc.{TaskMapper, TaskRepository}
import com.hevodata.scheduler.core.model.TaskDetails
import com.hevodata.scheduler.dto.task.Task
import org.slf4j.LoggerFactory

class SchedulerRegistry(taskRepository: TaskRepository) {

  private val LOG = LoggerFactory.getLogger(classOf[SchedulerRegistry])

  def register(task: Task): Unit = {
    this.register(List(task))
  }

  def register(tasks: List[Task]): Unit = {
    validate(tasks)
    val request: Map[String, List[TaskDetails]] = tasks.map(task => TaskMapper.toTaskDetails(task)).groupBy(taskDetails => taskDetails.namespace)
    for((namespace, list) <- request) {
      val allExisting: Map[String, TaskDetails] = taskRepository.get(namespace, list.map(taskDetails => taskDetails.key))
      val toAdd: List[TaskDetails] = list.filter(taskDetails => {
        val existingTaskDetails: Option[TaskDetails] = allExisting.get(taskDetails.key)
        existingTaskDetails.isEmpty || !existingTaskDetails.get.primaryParameters().equals(taskDetails.primaryParameters())
      }).map(taskDetails => {
        val existingTask: Option[TaskDetails] = allExisting.get(taskDetails.key)
        taskDetails.nextExecutionTime = if(existingTask.isEmpty) new Date() else taskDetails.calculateNextExecutionTime(existingTask.get.executionTime)
        taskDetails
      })
      taskRepository.add(toAdd)
    }
  }
  private def validate(tasks: List[Task]): Unit = {
    tasks.foreach(task => task.resolveClass())
  }

  def fetchKeys(namespace: String): List[String] = {
    taskRepository.fetchAll(namespace)
  }

  def deRegister(namespace: String, key: String): Unit = {
    taskRepository.delete(namespace, List(key))
    LOG.info("De-registered task with Namespace: %s Key: %s".format(namespace, key))
  }

  def nextExecutionTime(namespace: String, key: String): Date = {
    val data: Map[String, TaskDetails] = taskRepository.get(namespace, List(key))
    data.get(key).map(record => record.nextExecutionTime).orElse(Option(new Date)).get
  }
}
