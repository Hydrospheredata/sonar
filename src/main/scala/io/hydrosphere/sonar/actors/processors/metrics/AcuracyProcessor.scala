package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.terms.AcuracyMetricSpec

class AcuracyProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: AcuracyMetricSpec) extends AbstractBehavior[Processor.MetricMessage] {
  override def onMessage(msg: Processor.MetricMessage): Behavior[Processor.MetricMessage] = {
    context.log.warning("Why i've got message here? MetricMessage: {}", msg)
    this
  }
}
