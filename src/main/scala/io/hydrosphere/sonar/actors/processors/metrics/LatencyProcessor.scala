package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{LatencyMetricSpec, Metric, MetricLabels}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object LatencyProcessor {

  private case object TimerKey
  private case object Timeout extends Processor.MetricMessage


  def behavior(context: ActorContext[Processor.MetricMessage], metricSpec: LatencyMetricSpec, duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimerKey, Timeout, duration)
      active(0, 0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }

  def active(
    sum: Double,
    count: Long,
    saveToActors: Set[ActorRef[MetricWriter.Message]],
    payloads: List[ExecutionInformation],
    metricSpec: LatencyMetricSpec,
    timers: TimerScheduler[Processor.MetricMessage],
    context: ActorContext[Processor.MetricMessage],
    duration: FiniteDuration
  ): Behavior[Processor.MetricMessage] = {
    
    Behaviors.receiveMessage {
      case m: Processor.MetricRequest =>
        val (sumDelta, countDelta) = m.payload.metadata.map(_.latency -> 1).getOrElse(0D -> 0)
        active(sum + sumDelta, count + countDelta, saveToActors + m.saveTo, m.payload :: payloads, metricSpec, timers, context, duration)
        
      case Timeout =>
        context.log.debug("Timeout for latency buffering")
        val latency = if (count == 0) {
          0d
        } else {
          sum / count
        }
        val labels = MetricLabels(
          modelVersionId = metricSpec.modelVersionId,
          metricSpecId = metricSpec.id,
          traces = Traces.many(payloads.reverse),
          originTraces = OriginTraces.many(payloads.reverse)
        )
        val health = if (metricSpec.withHealth) {
          metricSpec.config.threshold.map(_ >= latency)
        } else None
        val metrics = payloads.headOption match {
          case Some(ei) => Seq(Metric("latency", latency, labels, health, ei.getTimestamp))
          case None => Seq(Metric("latency", latency, labels, health))
        }
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(metrics))
        active(0, 0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }
}
