package io.hydrosphere.sonar.actors.writers

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import cats.effect.IO
import io.hydrosphere.sonar.services.{AlertManagerService, MetricStorageService}
import io.hydrosphere.sonar.terms.Metric

object MetricWriter {
  sealed trait Message
  final case class ProcessedMetric(metrics: Seq[Metric]) extends Message

  def apply(metricStorageService: MetricStorageService[IO], alertingService: AlertManagerService[IO]): Behavior[Message] = Behaviors.setup { ctx => new MetricWriter(ctx, metricStorageService, alertingService) }
}

class MetricWriter(context: ActorContext[MetricWriter.Message], metricStorageService: MetricStorageService[IO], alertingService: AlertManagerService[IO]) extends AbstractBehavior[MetricWriter.Message] {
  override def onMessage(msg: MetricWriter.Message): Behavior[MetricWriter.Message] = msg match {
    case MetricWriter.ProcessedMetric(metrics) =>
      metricStorageService.saveMetrics(metrics).unsafeRunAsync {
        case Right(_) => context.log.info(s"Written ${metrics.size} metrics (${metrics.map(_.name).mkString(",")})")
        case Left(exc) => context.log.error(exc, "Error while writing metrics")
      }
      alertingService.sendMetric(metrics).unsafeRunAsync {
        case Right(_) => context.log.info(s"Alerts sent")
        case Left(exc) => context.log.error(exc, "Error while sending alerts")
      }
      this
  }
}