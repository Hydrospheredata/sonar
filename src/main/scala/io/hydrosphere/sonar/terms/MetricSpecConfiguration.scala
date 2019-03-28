package io.hydrosphere.sonar.terms

sealed trait MetricSpecConfiguration

case class InputMetricSpecConfiguration(input: String) extends MetricSpecConfiguration
case class ExternalDoubleMetricSpecConfiguration(input: String, applicationName: String, threshold: Option[Double]) extends MetricSpecConfiguration
case class ExternalSpecConfiguration(applicationName: String, threshold: Option[Double]) extends MetricSpecConfiguration
case class ExternalMetricSpecConfiguration(input: String, applicationName: String) extends MetricSpecConfiguration
case class HealthRateMetricSpecConfiguration(interval: Long, threshold: Option[Double]) extends MetricSpecConfiguration
case class CounterMetricSpecConfiguration(interval: Long) extends MetricSpecConfiguration