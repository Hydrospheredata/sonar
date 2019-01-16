package io.hydrosphere.sonar.terms

import java.time.Instant

case class Metric(name: String, value: Double, labels: Map[String, String], health: Option[Boolean] = None, timestamp: Long = Instant.now.toEpochMilli)
