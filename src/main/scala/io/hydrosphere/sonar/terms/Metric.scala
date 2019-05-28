package io.hydrosphere.sonar.terms

import java.time.Instant

case class Metric(name: String, value: Double, labels: Map[String, String], health: Option[Boolean] = None, timestamp: Long = Instant.now.toEpochMilli)

case class MetricsAggregation(meanValue: Option[Double], meanHealth:Option[Double], from: Long, till:Long, modelVersionId: Long, minValue:Option[Double],
maxValue:Option[Double])
