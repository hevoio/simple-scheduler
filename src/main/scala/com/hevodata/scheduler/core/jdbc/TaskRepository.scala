package com.hevodata.scheduler.core.jdbc

import java.sql.{PreparedStatement, ResultSet}
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import com.hevodata.scheduler.core.Constants
import com.hevodata.scheduler.core.model.{CronTaskDetails, RepeatableTaskDetails, TaskDetails, TaskStatus, TaskType}
import com.hevodata.scheduler.core.model.TaskStatus.Status
import com.hevodata.scheduler.core.model.TaskType.TaskType
import com.hevodata.scheduler.dto.ExecutionResponseData
import com.hevodata.scheduler.dto.task.{CronTask, RepeatableTask, Task}
import com.hevodata.scheduler.statsd.InfraStatsD
import com.hevodata.scheduler.util.Util

import javax.sql.DataSource
import org.slf4j.LoggerFactory

import java.time.ZoneId

/**
 *
 * @param _tablePrefix: The common prefix to be applied to all of the tracking tables
 */
class TaskRepository(_dataSource: DataSource, _tablePrefix: String) extends JdbcRepository(_dataSource) {

  private val LOG = LoggerFactory.getLogger(classOf[TaskRepository])

  var dataSource: DataSource = _dataSource
  var tablePrefix: String = _tablePrefix

  def add(tasks: List[TaskDetails]): Unit = {
    if(tasks.iterator.size > 0) {
      val sql = ("INSERT INTO %s(type, namespace, name, handler_class, schedule_expression, parameters, status, execution_time, next_execution_time) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
        "ON DUPLICATE KEY UPDATE schedule_expression = ?, parameters = ?, handler_class = ?, next_execution_time = FROM_UNIXTIME(?)").format(applicableTable(TaskRepository.tasksTable))
      super.batchUpdate(
        sql,
        tasks,
        (task: TaskDetails, statement: PreparedStatement) => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), task.discriminator().toString)
          statement.setString(counter.incrementAndGet(), task.namespace)
          statement.setString(counter.incrementAndGet(), task.key)
          statement.setString(counter.incrementAndGet(), task.handlerClassName)
          statement.setString(counter.incrementAndGet(), task.scheduleExpression)
          statement.setString(counter.incrementAndGet(), task.parameters)
          statement.setString(counter.incrementAndGet(), task.status.toString)

          statement.setString(counter.incrementAndGet(), task.scheduleExpression)
          statement.setString(counter.incrementAndGet(), task.parameters)
          statement.setString(counter.incrementAndGet(), task.handlerClassName)
          statement.setLong(counter.incrementAndGet(), Util.millisToSeconds(task.nextExecutionTime.getTime))
        }
      )
      InfraStatsD.count(InfraStatsD.Aspect.TASKS_ADDED, tasks.iterator.size, java.util.Arrays.asList())
    }
  }

  def get(namespace: String, keys: List[String]): Map[String, TaskDetails] = {
    var map: Map[String, TaskDetails] = Map()
    if(keys.iterator.size > 0) {
      val sql: String = "SELECT * FROM %s WHERE namespace = ? AND name IN (%s)".format(applicableTable(TaskRepository.tasksTable), List.fill(keys.length)("?").mkString(","))
      val results = super.query(
        sql,
        statement => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), namespace)
          keys.foreach(key => statement.setString(counter.incrementAndGet(), key))
        },
        resultSet => TaskMapper.toTaskDetails(resultSet)
      )
      map = results.map(taskDetails => (taskDetails.key, taskDetails)).toMap
    }
    map
  }

  def delete(namespace: String, keys: List[String]):Unit = {
    if(keys.iterator.size > 0) {
      val sql: String = "DELETE FROM %s WHERE namespace = ? AND name IN (%s)".format(applicableTable(TaskRepository.tasksTable), List.fill(keys.length)("?").mkString(","))
      super.update(
        sql,
        statement => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), namespace)
          keys.foreach(key => statement.setString(counter.incrementAndGet(), key))
        }
      )
      InfraStatsD.count(InfraStatsD.Aspect.TASKS_DELETED, keys.iterator.size, java.util.Arrays.asList())
    }
  }

  def fetch(statuses: List[Status], count: Int, futureSeconds: Int): List[TaskDetails] = {
    val sql: String = "SELECT * FROM %s WHERE status in (%s) AND next_execution_time < DATE_ADD(NOW(), INTERVAL ? SECOND) ORDER BY next_execution_time ASC LIMIT ?"
      .format(applicableTable(TaskRepository.tasksTable), List.fill(statuses.length)("?").mkString(","))
    super.query(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statuses.foreach(status => statement.setString(counter.incrementAndGet(), status.toString))
        statement.setInt(counter.incrementAndGet(), futureSeconds)
        statement.setInt(counter.incrementAndGet(), count)
      },
      resultSet => TaskMapper.toTaskDetails(resultSet)
    )
  }

  /**
   * @return All tasks. Only for tests
   */
  def fetchAll(): List[TaskDetails] = {
    val sql: String = "SELECT * FROM %s".format(applicableTable(TaskRepository.tasksTable))
    super.query(sql, _ => {}, resultSet => TaskMapper.toTaskDetails(resultSet))
  }

  def fetchAll(namespace: String): List[String] = {
    val sql: String = "SELECT name FROM %s WHERE namespace = ?".format(applicableTable(TaskRepository.tasksTable))
    super.query(
      sql,
      statement => statement.setString(1, namespace),
      resultSet => resultSet.getString(Constants.fieldName)
    )
  }

  def fetchPicked(pickedSince: Int): List[TaskDetails] = {
    val sql: String = "SELECT * FROM %s WHERE status = ? AND picked_at < DATE_ADD(NOW(), INTERVAL -? SECOND)".format(applicableTable(TaskRepository.tasksTable))
    super.query(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statement.setString(counter.incrementAndGet(), TaskStatus.PICKED.toString)
        statement.setInt(counter.incrementAndGet(), pickedSince)
      },
      resultSet => TaskMapper.toTaskDetails(resultSet)
    )
  }

  def markPicked(ids: List[Long], executorId: String): Unit = {
    if(ids.iterator.size > 0) {
      val sql: String = "UPDATE %s SET status = ?, executor_id = ?, picked_at = NOW(), executions = executions + 1 WHERE id IN (%s)"
        .format(applicableTable(TaskRepository.tasksTable), List.fill(ids.length)("?").mkString(","))
      super.update(
        sql,
        statement => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), TaskStatus.PICKED.toString)
          statement.setString(counter.incrementAndGet(), executorId)
          ids.foreach(id => statement.setLong(counter.incrementAndGet(), id))
        }
      )
    }
  }

  def update(fromStatus: Status, toStatus: Status, executorId: String): Unit = {
    val sql: String = "UPDATE %s SET status = ? WHERE executor_id = ? AND status = ?".format(applicableTable(TaskRepository.tasksTable))
    super.update(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statement.setString(counter.incrementAndGet(), toStatus.toString)
        statement.setString(counter.incrementAndGet(), executorId)
        statement.setString(counter.incrementAndGet(), fromStatus.toString)
      }
    )
  }

  def updateNextExecutionTime(namespace: String, key: String, nextExecutionTime: Date): Unit = {
    val sql: String = "UPDATE %s SET status = ?, next_execution_time = FROM_UNIXTIME(?) WHERE namespace = ? AND name = ? AND status <> ?".format(applicableTable(TaskRepository.tasksTable))
    super.update(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statement.setString(counter.incrementAndGet(), TaskStatus.SUCCEEDED.toString)
        statement.setLong(counter.incrementAndGet(), Util.millisToSeconds(nextExecutionTime.getTime))
        statement.setString(counter.incrementAndGet(), namespace)
        statement.setString(counter.incrementAndGet(), key)
        statement.setString(counter.incrementAndGet(), TaskStatus.PICKED.toString)
      }
    )
    InfraStatsD.incr(InfraStatsD.Aspect.TRIGGER_RUN, java.util.Arrays.asList())
  }

  def markExpired(ids: List[Long], bufferSeconds: Int): Unit = {
    val sql: String = ("UPDATE %s set status = ?, last_failed_at = NOW(), failure_count = failure_count + 1 " +
      "WHERE status = ? AND picked_at < DATE_ADD(NOW(), INTERVAL -? SECOND) AND id IN (%s)").format(applicableTable(TaskRepository.tasksTable), List.fill(ids.length)("?").mkString(","))
    super.update(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statement.setString(counter.incrementAndGet(), TaskStatus.EXPIRED.toString)
        statement.setString(counter.incrementAndGet(), TaskStatus.PICKED.toString)
        statement.setInt(counter.incrementAndGet(), bufferSeconds)
        ids.foreach(id => statement.setLong(counter.incrementAndGet(), id))
      }
    )
    InfraStatsD.count(InfraStatsD.Aspect.TASKS_CLEANED, ids.iterator.size, java.util.Arrays.asList())
  }

  def markFailed(data: List[ExecutionResponseData]): Unit = {
    val sql: String = "UPDATE %s SET status = ?, last_failed_at = NOW(), failure_count = failure_count + 1, execution_time = FROM_UNIXTIME(?), next_execution_time = FROM_UNIXTIME(?) WHERE id = ?"
      .format(applicableTable(TaskRepository.tasksTable))
    updateStatus(sql, data)
  }

  def markSucceeded(data: List[ExecutionResponseData]): Unit = {
    val sql = "UPDATE %s SET status = ?, failure_count = 0, execution_time = FROM_UNIXTIME(?), next_execution_time = FROM_UNIXTIME(?) WHERE id = ?".format(applicableTable(TaskRepository.tasksTable))
    updateStatus(sql, data)
  }

  private def updateStatus(sql: String, data: List[ExecutionResponseData]): Unit = {
    if(data.iterator.size > 0) {
      super.batchUpdate(
        sql,
        data,
        (responseData: ExecutionResponseData, statement: PreparedStatement) => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), responseData.targetStatus.toString)
          statement.setLong(counter.incrementAndGet(), Util.millisToSeconds(responseData.actualExecutionTime))
          statement.setLong(counter.incrementAndGet(), Util.millisToSeconds(responseData.nextExecutionTime))
          statement.setLong(counter.incrementAndGet(), responseData.id)
        }
      )
    }
  }

  private def applicableTable(table: String): String = {
    this.tablePrefix + table
  }
}

object TaskRepository {
  val tasksTable: String = "scheduled_tasks"
}

object TaskMapper {

  def toTaskDetails(resultSet: ResultSet): TaskDetails = {

    val taskType: TaskType = TaskType.withName(resultSet.getString(Constants.fieldType))

    val namespace: String = resultSet.getString(Constants.fieldNamespace)
    val key: String = resultSet.getString(Constants.fieldName)
    val scheduleExpression: String = resultSet.getString(Constants.fieldScheduleExpression)
    val handlerClassName: String = resultSet.getString(Constants.fieldHandlerClass)
    val timezone: ZoneId = ZoneId.of(resultSet.getString(Constants.fieldTimeZone))
    val task: TaskDetails = if(TaskType.CRON == taskType) CronTaskDetails(namespace, key, scheduleExpression, timezone, handlerClassName) else RepeatableTaskDetails(namespace, key, scheduleExpression, handlerClassName)

    task.id = resultSet.getLong(Constants.fieldId)
    task.parameters = resultSet.getString(Constants.fieldParameters)
    task.status = TaskStatus.withName(resultSet.getString(Constants.fieldStatus))
    task.pickedTime = resultSet.getTimestamp(Constants.fieldPickedTime)
    task.nextExecutionTime = resultSet.getTimestamp(Constants.fieldNextExecutionTime)
    task.executionTime = resultSet.getTimestamp(Constants.fieldExecutionTime)

    task.executions = resultSet.getLong(Constants.fieldExecutions)
    task.failures = resultSet.getInt(Constants.fieldFailures)

    task
  }

  def toTaskDetails(task: Task): TaskDetails = {
    val taskDetails: TaskDetails =
      task match {
      case cronTask: CronTask =>
        CronTaskDetails(cronTask.namespace, cronTask.key, cronTask.scheduleExpression(), cronTask.timezone, cronTask.handlerClassName)
      case repeatableTask: RepeatableTask =>
        RepeatableTaskDetails(repeatableTask.namespace, repeatableTask.key, repeatableTask.scheduleExpression(), repeatableTask.handlerClassName)
    }
    taskDetails.parameters = task.parameters
    taskDetails
  }
}
