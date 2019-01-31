package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.monitoring.ExecutionInformation
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{CounterMetricSpec, Metric}

import scala.concurrent.duration.FiniteDuration

object CounterProcessor {

  private case object TimerKey
  private case object Timeout extends Processor.MetricMessage


  def behavior(context: ActorContext[Processor.MetricMessage], metricSpec: CounterMetricSpec, duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimerKey, Timeout, duration)
      active(0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }

  def active(
    count: Long,
    saveToActors: Set[ActorRef[MetricWriter.Message]],
    payloads: List[ExecutionInformation],
    metricSpec: CounterMetricSpec,
    timers: TimerScheduler[Processor.MetricMessage],
    context: ActorContext[Processor.MetricMessage],
    duration: FiniteDuration
  ): Behavior[Processor.MetricMessage] = {

    Behaviors.receiveMessage {
      case m: Processor.MetricRequest =>
        active(count + 1, saveToActors + m.saveTo, m.payload :: payloads, metricSpec, timers, context, duration)
      case Timeout =>
        context.log.debug("Timeout for counter buffering")
        val labels = Map(
          "modelVersionId" -> metricSpec.modelVersionId.toString,
          "traces" -> Traces.many(payloads.reverse)
        )
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(Seq(Metric("counter", count.toDouble, labels, None))))
        active(0, Set.empty, List.empty, metricSpec, timers, context, duration)
    }
  }

}
