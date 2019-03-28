package io.hydrosphere.sonar.actors.writers

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import cats.effect.IO
import io.hydrosphere.sonar.services.MetricStorageService
import io.hydrosphere.sonar.terms.Metric

object MetricWriter {
  sealed trait Message
  final case class ProcessedMetric(metrics: Seq[Metric]) extends Message

  def apply(metricStorageService: MetricStorageService[IO]): Behavior[Message] = Behaviors.setup { ctx => new MetricWriter(ctx, metricStorageService) }
}

class MetricWriter(context: ActorContext[MetricWriter.Message], metricStorageService: MetricStorageService[IO]) extends AbstractBehavior[MetricWriter.Message] {
  override def onMessage(msg: MetricWriter.Message): Behavior[MetricWriter.Message] = msg match {
    case MetricWriter.ProcessedMetric(metrics) =>
      metricStorageService.saveMetrics(metrics).unsafeRunAsync {
        case Right(_) => context.log.info(s"Written ${metrics.size} metrics")
        case Left(exc) => context.log.error(exc, "Error while writing metrics")
      }
      this
  }
}