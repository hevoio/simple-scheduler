package io.hevo.scheduler.core.jdbc

import java.sql.{PreparedStatement, ResultSet}

import io.hevo.scheduler.util.Util
import javax.sql.DataSource

import scala.collection.mutable
import scala.util.{Try, Using}

abstract class JdbcRepository(dataSource: DataSource) {

  def update(sql: String, applyParameters: PreparedStatement => Unit): Unit = {
    Util.throwOnError(Using(this.dataSource.getConnection) {
      connection =>
        Util.throwOnError(Using(connection.prepareStatement(sql)) {
          statement => {
            applyParameters(statement)
            statement.execute()
          }
        })
    })
  }

  def batchUpdate[T](sql: String, inputs: List[T], applyParameters: (T, PreparedStatement) => Unit): Unit = {
    Util.throwOnError(Using(this.dataSource.getConnection) {
      connection =>
        Util.throwOnError(Using(connection.prepareStatement(sql)) {
          statement => {
            inputs.foreach(input => {
              applyParameters(input, statement)
              statement.addBatch()
            })
            statement.executeBatch()
          }
        })
    })
  }

  def query[T](sql: String, applyParameters: PreparedStatement => Unit, translate: ResultSet => T): List[T] = {
    val list: mutable.ListBuffer[T] = mutable.ListBuffer()
    Util.throwOnError(Using(this.dataSource.getConnection) {
      connection =>
        Util.throwOnError(Using(connection.prepareStatement(sql)) {
          statement => {
            applyParameters(statement)
            Util.throwOnError(Using(statement.executeQuery()) {
              resultSet =>
                while (resultSet.next) {
                  list += translate(resultSet)
                }
            })
          }
        })
    })
    list.toList
  }
}