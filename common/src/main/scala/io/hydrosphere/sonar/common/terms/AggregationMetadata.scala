package io.hydrosphere.sonar.common.terms

case class AggregationMetadata(id: String, modelVersionId: Long, firstCheckId: String, lastCheckId: String, modelName: String, requestCount: Int, startTimestamp: Long, endTimestamp: Long)
