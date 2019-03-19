package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import io.hydrosphere.serving.monitoring.monitoring.ExecutionInformation
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
        val (sumDelta, countDelta) = m.payload.responseOrError match {
          case ResponseOrError.Response(value) =>
            val latency = for {
              tensor <- value.internalInfo.get("system.latency")
              l <- Try(tensor.int64Val).toOption
              head <- l.headOption
            } yield head.toDouble
            latency match {
              case Some(l) => (l, 1)
              case None =>
                context.log.debug("Empty system.latency")
                context.log.debug(s"${value.internalInfo}")
                (0.0, 0)
            }
          case x =>
            context.log.debug("Response: {}", x.getClass)
            (0.0, 0)
        }
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
