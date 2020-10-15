package io.hydrosphere.sonar.common.terms

import io.hydrosphere.serving.monitoring.metadata.TraceData

case class FeatureCheckAggregation(checks: Long, passed: Long)
case class CheckAggregation(modelVersionId: Long, features: Map[String, FeatureCheckAggregation], requests: Long, firstTraceData: TraceData, lastTraceData: TraceData)
