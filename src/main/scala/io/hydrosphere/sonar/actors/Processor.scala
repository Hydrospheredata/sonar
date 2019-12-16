package io.hydrosphere.sonar.actors

import io.hydrosphere.serving.monitoring.api.ExecutionInformation

object Processor {
  trait ProfileMessage
  final case class ProfileRequest(payload: ExecutionInformation) extends ProfileMessage
  
  trait CheckMessage
  final case class CheckRequest(payload: ExecutionInformation) extends CheckMessage
}
