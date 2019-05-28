package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.monitoring.api.ExecutionInformation.ResponseOrError
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{LatencyMetricSpec, Metric}

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
        val labels = Map(
          "modelVersionId" -> metricSpec.modelVersionId.toString,
          "traces" -> Traces.many(payloads.reverse)
        )
        val health = if (metricSpec.withHealth) {
          metricSpec.config.threshold.map(_ >= latency)
        } else None
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(Seq(Metric("latency", latency, labels, health))))
        active(0, 0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }
}
