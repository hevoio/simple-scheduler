package io.hevo.scheduler.core.jdbc

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import io.hevo.scheduler.core.Constants
import io.hevo.scheduler.core.model.{CronTaskDetails, RepeatableTaskDetails, TaskDetails, TaskStatus, TaskType}
import io.hevo.scheduler.core.model.TaskStatus.Status
import io.hevo.scheduler.core.model.TaskType.TaskType
import io.hevo.scheduler.dto.ExecutionResponseData
import io.hevo.scheduler.dto.task.{CronTask, Task}
import io.hevo.scheduler.util.Util
import javax.sql.DataSource
import org.slf4j.LoggerFactory

/**
 *
 * @param _tablePrefix: The common prefix to be applied to all of the tracking tables
 */
class TaskRepository(_dataSource: DataSource, _tablePrefix: String) extends JdbcRepository(_dataSource) {

  private val LOG = LoggerFactory.getLogger(classOf[TaskRepository])

  var dataSource: DataSource = _dataSource
  var tablePrefix: String = _tablePrefix

  def add(tasks: List[TaskDetails]): Unit = {
    if(tasks.nonEmpty) {
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
    }
  }

  def get(namespace: String, keys: List[String]): Map[String, TaskDetails] = {
    var map: Map[String, TaskDetails] = Map()
    if(keys.nonEmpty) {
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
    if(keys.nonEmpty) {
      val sql: String = "DELETE FROM %s WHERE namespace = ? AND name IN (%s)".format(applicableTable(TaskRepository.tasksTable), List.fill(keys.length)("?").mkString(","))
      super.update(
        sql,
        statement => {
          val counter: AtomicInteger = new AtomicInteger()
          statement.setString(counter.incrementAndGet(), namespace)
          keys.foreach(key => statement.setString(counter.incrementAndGet(), key))
        }
      )
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

  def markPicked(ids: List[Long], executorId: String): Unit = {
    if(ids.nonEmpty) {
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
  }

  def markExpired(fromStatus: Status, pickedSince: Int): Unit = {
    val sql: String = ("UPDATE %s set status = ?, last_failed_at = NOW(), failure_count = failure_count + 1 " +
      "WHERE status = ? AND picked_at < DATE_ADD(NOW(), INTERVAL -? SECOND)").format(applicableTable(TaskRepository.tasksTable))
    super.update(
      sql,
      statement => {
        val counter: AtomicInteger = new AtomicInteger()
        statement.setString(counter.incrementAndGet(), TaskStatus.EXPIRED.toString)
        statement.setString(counter.incrementAndGet(), fromStatus.toString)
        statement.setInt(counter.incrementAndGet(), pickedSince)
      }
    )
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
    if(data.nonEmpty) {
      data.foreach(record => LOG.info("Id: %d Status: %s at %s".format(record.id, record.targetStatus.toString, new SimpleDateFormat("dd-MMM hh:mm:ss:SSS").format(new Date()))))
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
    val task: TaskDetails = if(TaskType.CRON == taskType) CronTaskDetails(namespace, key, scheduleExpression, handlerClassName) else RepeatableTaskDetails(namespace, key, scheduleExpression, handlerClassName)

    task.id = resultSet.getLong(Constants.fieldId)
    task.parameters = resultSet.getString(Constants.fieldParameters)
    task.status = TaskStatus.withName(resultSet.getString(Constants.fieldStatus))
    task.nextExecutionTime = resultSet.getTimestamp(Constants.fieldNextExecutionTime)
    task.executionTime = resultSet.getTimestamp(Constants.fieldExecutionTime)

    task.executions = resultSet.getLong(Constants.fieldExecutions)
    task.failures = resultSet.getInt(Constants.fieldFailures)

    task
  }

  def toTaskDetails(task: Task): TaskDetails = {
    val taskDetails: TaskDetails = (if(task.isInstanceOf[CronTask]) classOf[CronTaskDetails] else classOf[RepeatableTaskDetails])
      .getDeclaredConstructor(classOf[String], classOf[String], classOf[String], classOf[String])
      .newInstance(task.namespace, task.key, task.scheduleExpression(), task.handlerClassName)
    taskDetails.parameters = task.parameters
    taskDetails
  }
}
