package io.hydrosphere.sonar.actors

import akka.actor.typed.ActorRef
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.actors.writers.MetricWriter

object Processor {
  trait MetricMessage
  final case class MetricRequest(payload: ExecutionInformation, saveTo: ActorRef[MetricWriter.Message]) extends MetricMessage
  
  trait ProfileMessage
  final case class ProfileRequest(payload: ExecutionInformation) extends ProfileMessage
  
  trait CheckMessage
  final case class CheckRequest(payload: ExecutionInformation) extends CheckMessage
}
