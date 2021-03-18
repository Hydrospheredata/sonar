package io.hydrosphere.sonar.terms

case class Check(check: Boolean, description: String, value: Double, threshold: Double, metricSpecId: Option[String] = None)
