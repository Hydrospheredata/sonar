package io.hydrosphere.sonar.common.terms

import io.hydrosphere.serving.monitoring.metadata.TraceData

case class Check(check: Boolean, description: String, value: Double, threshold: Double, metricSpecId: Option[String] = None)
