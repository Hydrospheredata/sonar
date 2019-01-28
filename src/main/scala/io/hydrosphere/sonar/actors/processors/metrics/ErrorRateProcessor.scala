package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{ErrorRateMetricSpec, Metric}

import scala.concurrent.duration.FiniteDuration

object ErrorRateProcessor {

  private case object TimerKey
  private case object Timeout extends Processor.MetricMessage

  def behavior(context: ActorContext[Processor.MetricMessage], metricSpec: ErrorRateMetricSpec, duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.withTimers { timers =>
      timers.startPeriodicTimer(TimerKey, Timeout, duration)
      active(0, Set.empty, metricSpec, timers, context, duration)
    }
  }

  def active(count: Long, saveToActors: Set[ActorRef[MetricWriter.Message]], metricSpec: ErrorRateMetricSpec, timers: TimerScheduler[Processor.MetricMessage], context: ActorContext[Processor.MetricMessage], duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.MetricRequest =>
        if (m.payload.responseOrError.isError) {
          active(count + 1, saveToActors + m.saveTo, metricSpec, timers, context, duration)
        } else {
          active(count, saveToActors + m.saveTo, metricSpec, timers, context, duration)
        }
      case Timeout =>
        context.log.debug("Timeout for error rate buffering")
        val labels = Map(
          "modelVersionId" -> metricSpec.modelVersionId.toString
        )
        val health = if (metricSpec.withHealth) {
          metricSpec.config.threshold.map(_ > count.toDouble)
        } else None
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(Seq(Metric("error_rate", count.toDouble, labels, health))))
        active(0, Set.empty, metricSpec, timers, context, duration)
    }
  }
}
