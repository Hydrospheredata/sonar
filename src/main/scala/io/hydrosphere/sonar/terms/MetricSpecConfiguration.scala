package io.hydrosphere.sonar.terms

sealed trait ThresholdCmpOperator
case object Eq extends ThresholdCmpOperator
case object NotEq extends ThresholdCmpOperator
case object Greater extends ThresholdCmpOperator
case object Less extends ThresholdCmpOperator
case object GreaterEq extends ThresholdCmpOperator
case object LessEq extends ThresholdCmpOperator

sealed trait MetricSpecConfiguration

case class InputMetricSpecConfiguration(input: String) extends MetricSpecConfiguration
case class ExternalDoubleMetricSpecConfiguration(input: String, applicationName: String, threshold: Option[Double]) extends MetricSpecConfiguration
case class ExternalSpecConfiguration(applicationName: String, threshold: Option[Double]) extends MetricSpecConfiguration
case class ExternalMetricSpecConfiguration(input: String, applicationName: String) extends MetricSpecConfiguration
case class HealthRateMetricSpecConfiguration(interval: Long, threshold: Option[Double]) extends MetricSpecConfiguration
case class CounterMetricSpecConfiguration(interval: Long) extends MetricSpecConfiguration
case class EmptyMetricSpecConfiguration() extends MetricSpecConfiguration
case class CustomModelMetricSpecConfiguration(applicationName: String, threshold: Option[Double], thresholdCmpOperator: Option[ThresholdCmpOperator]) extends MetricSpecConfiguration