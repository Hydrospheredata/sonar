package io.hydrosphere.sonar.utils

sealed trait SonarError extends Throwable

final case class MetricSpecNotFound(id: String) extends SonarError
final case class UnknownMetricSpecKind(kind: String) extends SonarError

