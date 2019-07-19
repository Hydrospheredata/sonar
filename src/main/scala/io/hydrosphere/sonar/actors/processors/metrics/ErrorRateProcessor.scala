package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{ErrorRateMetricSpec, Metric, MetricLabels}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

import scala.concurrent.duration.FiniteDuration

object ErrorRateProcessor {

  private case object TimerKey
  private case object Timeout extends Processor.MetricMessage

  def behavior(context: ActorContext[Processor.MetricMessage], metricSpec: ErrorRateMetricSpec, duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimerKey, Timeout, duration)
      active(0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }
  

  def active(
    count: Long,
    saveToActors: Set[ActorRef[MetricWriter.Message]],
    payloads: List[ExecutionInformation],
    metricSpec: ErrorRateMetricSpec,
    timers: TimerScheduler[Processor.MetricMessage],
    context: ActorContext[Processor.MetricMessage],
    duration: FiniteDuration
  ): Behavior[Processor.MetricMessage] = {
    
    Behaviors.receiveMessage {
      
      case m: Processor.MetricRequest =>
        val inc = if (m.payload.responseOrError.isError) 1 else 0
        val nextPayloads = m.payload :: payloads
        active(count + inc, saveToActors + m.saveTo, nextPayloads, metricSpec, timers, context, duration)
        
      case Timeout =>
        context.log.debug("Timeout for error rate buffering")
        val labels = MetricLabels(
          modelVersionId = metricSpec.modelVersionId,
          metricSpecId = metricSpec.id,
          traces = Traces.many(payloads.reverse),
          originTraces = OriginTraces.many(payloads.reverse)
        )
        val health = if (metricSpec.withHealth) {
          metricSpec.config.threshold.map(_ > count.toDouble)
        } else None
        val metrics = payloads.headOption match {
          case Some(ei) => Seq(Metric("error_rate", count.toDouble, labels, health, ei.getTimestamp))
          case None => Seq.empty[Metric]
        }
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(metrics))
        active(0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }
}
