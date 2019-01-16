package io.hydrosphere.sonar.terms

sealed trait MetricSpecConfiguration

case class InputMetricSpecConfiguration(input: String) extends MetricSpecConfiguration
case class ExternalDoubleMetricSpecConfiguration(input: String, applicationName: String, applicationSignature: String, threshold: Option[Double]) extends MetricSpecConfiguration
case class ExternalMetricSpecConfiguration(input: String, applicationName: String, applicationSignature: String) extends MetricSpecConfiguration
