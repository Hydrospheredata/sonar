package io.hydrosphere.sonar.common.config

case class MongoConfiguration(
  host: String,
  port: Int,
  database: String,
  user: Option[String],
  pass: Option[String],
  authDb: Option[String]
)
