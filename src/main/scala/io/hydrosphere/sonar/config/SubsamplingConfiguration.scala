package io.hydrosphere.sonar.config

sealed trait SubsamplingConfiguration

case class Reservoir(size: Int) extends SubsamplingConfiguration
