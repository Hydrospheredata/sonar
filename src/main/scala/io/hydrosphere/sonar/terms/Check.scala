package io.hydrosphere.sonar.terms

import io.hydrosphere.serving.monitoring.metadata.TraceData

case class Check(modelVersionId: Long, feature: String, check: Boolean, trace: Option[TraceData], value: Long, description: String)
