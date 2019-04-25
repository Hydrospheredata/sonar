package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.terms.AccuracyMetricSpec

class AccuracyProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: AccuracyMetricSpec) extends AbstractBehavior[Processor.MetricMessage] {
  override def onMessage(msg: Processor.MetricMessage): Behavior[Processor.MetricMessage] = {
    this
  }
}
