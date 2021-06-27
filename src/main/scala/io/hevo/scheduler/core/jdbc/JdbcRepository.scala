package io.hevo.scheduler.core.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import javax.sql.DataSource

import scala.collection.mutable

abstract class JdbcRepository(dataSource: DataSource) {

  def update(sql: String, applyParameters: PreparedStatement => Unit): Unit = {
    var connection: Connection = null
    try {
      connection = this.dataSource.getConnection()
      var statement: PreparedStatement = null
      try {
        statement = connection.prepareStatement(sql)
        applyParameters(statement)
        statement.execute()
      }
      finally {
        statement.close()
      }
    }
    finally {
      connection.close()
    }
  }

  def batchUpdate[T](sql: String, inputs: List[T], applyParameters: (T, PreparedStatement) => Unit): Array[Int] = {
    var connection: Connection = null
    try {
      connection = this.dataSource.getConnection()
      var statement: PreparedStatement = null
      try {
        statement = connection.prepareStatement(sql)
        inputs.foreach(input => {
          applyParameters(input, statement)
          statement.addBatch()
        })
        statement.executeBatch()
      }
      finally {
        statement.close()
      }
    }
    finally {
      connection.close()
    }
  }

  def query[T](sql: String, applyParameters: PreparedStatement => Unit, translate: ResultSet => T): List[T] = {
    val list: mutable.ListBuffer[T] = mutable.ListBuffer()
    var connection: Connection = null
    try {
      connection = this.dataSource.getConnection()
      var statement: PreparedStatement = null
      try {
        statement = connection.prepareStatement(sql)
        applyParameters(statement)
        var resultSet: ResultSet = null
        try {
          resultSet = statement.executeQuery()
          while (resultSet.next) {
            list += translate(resultSet)
          }
        }
        finally {
          resultSet.close()
        }
      }
      finally {
        statement.close()
      }
    }
    finally {
      connection.close()
    }
    list.toList
  }
}