package io.hydrosphere.sonar.utils

sealed trait SonarError extends Throwable

final case class MetricSpecNotFound(id: String) extends SonarError
final case class UnknownMetricSpecKind(kind: String) extends SonarError

final case class CsvRowSizeMismatch(rowIndex: Long, headerLength: Int, row: Vector[String]) extends SonarError {
  override def getMessage: String = 
    s"CSV row at index $rowIndex has ${row.length} items, header has $headerLength. Row: $row"
}

final case class ProfileIsAlreadyProcessing(modelVersionId: Long) extends SonarError