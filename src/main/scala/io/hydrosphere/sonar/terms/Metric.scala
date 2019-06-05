package io.hydrosphere.sonar.terms

import java.time.Instant

import io.hydrosphere.serving.monitoring.metadata.TraceData

case class MetricLabels(modelVersionId: Long, metricSpecId: String, traces: Seq[Option[TraceData]], originTraces: Seq[Option[TraceData]], columnIndex: Option[Int] = None)

case class Metric(name: String, value: Double, labels: MetricLabels, health: Option[Boolean] = None, timestamp: Long = Instant.now.toEpochMilli)

case class MetricsAggregation(meanValue: Option[Double], meanHealth:Option[Double], from: Long, till:Long, modelVersionId: Long, minValue:Option[Double], maxValue:Option[Double])
