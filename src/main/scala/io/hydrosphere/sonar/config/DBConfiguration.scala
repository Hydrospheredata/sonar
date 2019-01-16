package io.hydrosphere.sonar.config

sealed trait DBConfiguration {
  val jdbcUrl: String
  val user: String
  val pass: String
}

case class Postgres(jdbcUrl: String, user: String, pass: String) extends DBConfiguration
case class H2(jdbcUrl: String, user: String, pass: String) extends DBConfiguration
