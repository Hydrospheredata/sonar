package io.hydrosphere.sonar.terms

import java.util.UUID

sealed trait MetricSpec {
  def name: String
  def modelVersionId: Long
  def config: MetricSpecConfiguration
  def withHealth: Boolean
  def id: String
}

case class KSMetricSpec(name: String, modelVersionId: Long, config: InputMetricSpecConfiguration, withHealth: Boolean = false, id: String = UUID.randomUUID().toString) extends MetricSpec
case class RFMetricSpec(name: String, modelVersionId: Long, config: ExternalDoubleMetricSpecConfiguration, withHealth: Boolean = false, id: String = UUID.randomUUID().toString) extends MetricSpec
case class AEMetricSpec(name: String, modelVersionId: Long, config: ExternalDoubleMetricSpecConfiguration, withHealth: Boolean = false, id: String = UUID.randomUUID().toString) extends MetricSpec
case class GANMetricSpec(name: String, modelVersionId: Long, config: ExternalMetricSpecConfiguration, withHealth: Boolean = false, id: String = UUID.randomUUID().toString) extends MetricSpec
case class LatencyMetricSpec(name: String, modelVersionId: Long, config: LatencyMetricSpecConfiguration, withHealth: Boolean = false, id: String = UUID.randomUUID().toString) extends MetricSpec