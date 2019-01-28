package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.monitoring.ExecutionInformation.ResponseOrError
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
      active(0, 0, Set.empty, metricSpec, timers, context, duration)
    }
  }

  def active(sum: Double, count: Long, saveToActors: Set[ActorRef[MetricWriter.Message]], metricSpec: LatencyMetricSpec, timers: TimerScheduler[Processor.MetricMessage], context: ActorContext[Processor.MetricMessage], duration: FiniteDuration): Behavior[Processor.MetricMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.MetricRequest =>
        m.payload.responseOrError match {
          case ResponseOrError.Empty => 
            context.log.debug("Empty response")
            active(sum, count, saveToActors + m.saveTo, metricSpec, timers, context, duration)
          case ResponseOrError.Error(value) =>
            context.log.debug("Error response")
            active(sum, count, saveToActors + m.saveTo, metricSpec, timers, context, duration)
          case ResponseOrError.Response(value) =>
            val latency = value.internalInfo.get("system.latency").flatMap(x => Try(x.doubleVal).toOption).flatMap(_.headOption)
            latency match {
              case Some(l) => active(sum + l, count + 1, saveToActors + m.saveTo, metricSpec, timers, context, duration)
              case None =>
                context.log.debug("Empty system.latency")
                context.log.debug(s"${value.internalInfo}")
                active(sum, count, saveToActors + m.saveTo, metricSpec, timers, context, duration)
            }
        }
      case Timeout =>
        context.log.debug("Timeout for latency buffering")
        val latency = if (count == 0) {
          0d
        } else {
          sum / count
        }
        val labels = Map(
          "modelVersionId" -> metricSpec.modelVersionId.toString
        )
        val health = if (metricSpec.withHealth) {
          metricSpec.config.threshold.map(_ >= latency)
        } else None
        saveToActors.foreach(_ ! MetricWriter.ProcessedMetric(Seq(Metric("latency", latency, labels, health))))
        active(0, 0, Set.empty, metricSpec, timers, context, duration)
    }
  }
}
