package io.hydrosphere.sonar.actors

import io.hydrosphere.monitoring.proto.sonar.entities.ExecutionInformation

object Processor {
  trait ProfileMessage
  final case class ProfileRequest(payload: ExecutionInformation) extends ProfileMessage
  
  trait CheckMessage
  final case class CheckRequest(payload: ExecutionInformation) extends CheckMessage
}
