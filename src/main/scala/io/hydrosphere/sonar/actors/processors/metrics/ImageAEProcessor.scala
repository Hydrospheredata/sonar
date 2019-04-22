package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.{MetricMessage, MetricRequest}
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms.{ImageAEMetricSpec, Metric}

class ImageAEProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: ImageAEMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {

  override def onMessage(msg: MetricMessage): Behavior[MetricMessage] = msg match {
    case MetricRequest(payload, saveTo) =>
      val maybeRequest = for {
        request <- payload.request
        inputs = request.inputs
        response <- payload.responseOrError.response
        outputs = response.outputs
      } yield inputs ++ outputs
      maybeRequest match {
        case Some(request) =>
          predictionService.callApplication(metricSpec.config.applicationName, request).unsafeRunAsync {
            case Right(value) =>
              val reconstructed = value.outputs.get("score").flatMap(_.doubleVal.headOption).getOrElse(0d)
              val health = if (metricSpec.withHealth) {
                Some(reconstructed <= metricSpec.config.threshold.getOrElse(Double.MaxValue))
              } else None
              val metric = Metric(
                "image_autoencoder_reconstructed", reconstructed,
                Map(
                  "modelVersionId" -> metricSpec.modelVersionId.toString,
                  "trace" -> Traces.single(payload)),
                health)
              saveTo ! MetricWriter.ProcessedMetric(Seq(metric))
            case Left(exc) => context.log.error(exc, s"Error while requesting Image AE (${metricSpec.config.applicationName}) prediction for modelVersion ${metricSpec.modelVersionId}")
          }
        case None => context.log.warning("ImageAutoencoder: request or response is empty")
      }
      this
  }

}