package io.hevo.scheduler.core.jdbc

import java.sql.ResultSet
import java.util

import io.hevo.scheduler.core.Constants
import io.hevo.scheduler.core.model.{CronTask, RepeatableTask, Task, TaskStatus, TaskType}
import io.hevo.scheduler.core.model.TaskStatus.Status
import io.hevo.scheduler.core.model.TaskType.TaskType
import io.hevo.scheduler.dto.TaskSuccessData
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext

import scala.jdk.javaapi.CollectionConverters


class TaskRepository private(tablePrefix: String) {

  var orm: Jdbi = _
  def this(orm: Jdbi, tablePrefix: String = "") {
    this(tablePrefix)
    this.orm = orm
    orm.registerRowMapper(new TaskMapper)
  }

  def add(tasks: List[Task]): Unit = {
    val sql = "INSERT INTO :table_name(type, name, handler_class, schedule_expression, status, previous_execution_time, next_execution_time) " +
      "VALUES (:type, :name, :handler_class, :schedule, :status, NOW(), NOW()) " +
      "ON DUPLICATE KEY UPDATE schedule_expression = :schedule_expression, next_execution_time = :next_execution_time, status = :status"
    orm.withHandle(handle => {
      val batch = handle.prepareBatch(sql)
      tasks.foreach(record => {
        batch
          .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
          .bind(Constants.fieldType, record.discriminator())
          .bind(Constants.fieldName, record.qualifiedName())
          .bind(Constants.fieldHandlerClass, record.handlerClassName)
          .bind(Constants.fieldSchedule, record.scheduleExpression)
          .bind(Constants.fieldStatus, TaskStatus.INIT)
          .bind(Constants.fieldNextExecutionTime, record.nextExecutionTime)
      })
      batch.execute
    })
  }

  def get(namespace: String, keys: List[String]): Map[String, Task] = {
    val qualifiedNames: List[String] = keys.map(key => Task.toQualifiedName(namespace, key))
    val query = "SELECT * FROM :table_name WHERE name in (<name>)"
    val list: util.List[Task] = this.orm.withHandle(handle => handle.createQuery(query)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bindList(Constants.fieldName, qualifiedNames)
      .mapTo(classOf[Task]).list())
    CollectionConverters.asScala(list).map(task => (task.key, task)).toMap
  }

  def delete(namespace: String, keys: List[String]):Unit = {
    val qualifiedNames: List[String] = keys.map(key => Task.toQualifiedName(namespace, key))
    val sql = "DELETE FROM :table_name WHERE name in (<name>)"
    orm.withHandle(handle => handle.createUpdate(sql)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bindList(Constants.fieldName, CollectionConverters.asJava(qualifiedNames))
      .execute
    )
  }

  def fetch(statuses: util.List[Status], count: Int, futureSeconds: Int): List[Task] = {
    val query = "SELECT * FROM :table_name WHERE status in (<status>) AND next_execution_time < DATE_ADD(NOW(), INTERVAL :seconds SECOND) ORDER BY next_execution_time LIMIT :limit"

    val list: util.List[Task] = this.orm.withHandle(handle => handle.createQuery(query)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bindList(Constants.fieldStatus, statuses)
      .bind(Constants.fieldSeconds, futureSeconds)
      .bind(Constants.fieldLimit, count)
      .mapTo(classOf[Task]).list())

    CollectionConverters.asScala(list).toList
  }

  def markPicked(ids: List[Long], executorId: String): Unit = {
    val sql = "UPDATE :table_name set status = :status, executor_id = :executor_id WHERE id IN (<ids>)"
    orm.withHandle(handle => handle.createUpdate(sql)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bind(Constants.fieldExecutorId, executorId)
      .bind(Constants.fieldStatus, TaskStatus.PICKED)
      .bindList(Constants.fieldIds, CollectionConverters.asJava(ids))
      .execute
    )
  }

  def update(fromStatus: Status, toStatus: Status, executorId: String): Unit = {
    val sql = "UPDATE :table_name SET status = :to_status WHERE executor_id = :executor_id AND status = :from_status"
    orm.withHandle(handle => handle.createUpdate(sql)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bind(Constants.fieldFromStatus, fromStatus)
      .bind(Constants.fieldToStatus, toStatus)
      .bind(Constants.fieldExecutorId, executorId)
      .execute
    )
  }

  def markFailed(ids: List[Long], targetStatus: Status): Unit = {
    val sql = "UPDATE :table_name set status = :status, last_failed_at = NOW(), failure_count = failure_count + 1 WHERE id IN (<ids>)"
    orm.withHandle(handle => handle.createUpdate(sql)
      .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
      .bind(Constants.fieldStatus, targetStatus)
      .bindList(Constants.fieldIds, CollectionConverters.asJava(ids))
      .execute
    )
  }

  def markSucceeded(data: List[TaskSuccessData]): Unit = {
    val sql = "UPDATE :table_name set status = :status, failure_count = 0, previous_execution_time = UNIX_TIMESTAMP(:execution_time), next_execution_time = UNIX_TIMESTAMP(:next_execution_time) WHERE id in (<ids>)"
    orm.withHandle(handle => {
      val batch = handle.prepareBatch(sql)
      data.foreach(record => {
        batch
          .bind(Constants.fieldTableName, applicableTable(TaskRepository.tasksTable))
          .bind(Constants.fieldStatus, TaskStatus.SUCCEEDED)
          .bind(Constants.fieldExecutionTime, record.actualExecutionTime)
          .bind(Constants.fieldNextExecutionTime, record.nextExecutionTime)
          .bind(Constants.fieldId, record.id)
      })
      batch.execute
    })
  }

  private def applicableTable(table: String): String = {
    this.tablePrefix + table
  }
}

object TaskRepository {
  val tasksTable: String = "scheduled_tasks"
}

class TaskMapper extends RowMapper[Task] {

  override def map(resultSet: ResultSet, ctx: StatementContext): Task = {
    val taskType: TaskType = TaskType.withName(resultSet.getString(Constants.fieldType))

    val id: Long = resultSet.getLong(Constants.fieldId)
    val namespaceKeyTuple: (String, String) = Task.fromQualifiedName(resultSet.getString(Constants.fieldName))
    val scheduleExpression: String = resultSet.getString(Constants.fieldSchedule)
    val task: Task = if(TaskType.CRON == taskType) CronTask(id, namespaceKeyTuple._1, namespaceKeyTuple._2, scheduleExpression) else RepeatableTask(id, namespaceKeyTuple._1, namespaceKeyTuple._2, scheduleExpression)

    task.handlerClassName = resultSet.getString(Constants.fieldType)
    val params = resultSet.getObject(Constants.fieldParameters)
    task.parameters = if(null == params) null else params.asInstanceOf[Map[String, _]]
    task.status = TaskStatus.withName(resultSet.getString(Constants.fieldStatus))
    task.nextExecutionTime = resultSet.getDate(Constants.fieldNextExecutionTime)
    task.executionTime = resultSet.getDate(Constants.fieldExecutionTime)

    task
  }
}
