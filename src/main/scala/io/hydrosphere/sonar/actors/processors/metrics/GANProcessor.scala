package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.serving.tensorflow.TensorShape
import io.hydrosphere.serving.tensorflow.tensor.DoubleTensor
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms.{GANMetricSpec, Metric}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

class GANProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: GANMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {
  override def onMessage(msg: Processor.MetricMessage): Behavior[Processor.MetricMessage] = msg match {
    case Processor.MetricRequest(payload, saveTo) =>
      context.log.debug(s"Computing GAN: $payload")
      predictionService.callApplication(metricSpec.config.applicationName,
        inputs = Map(
          "client_profile" -> DoubleTensor(TensorShape.vector(-1), payload.getDoubleInput(metricSpec.config.input)).toProto
        )
      ).unsafeRunAsync {
        case Right(value) =>
          val outlier = value.outputs.get("class_one").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val inlier = value.outputs.get("class_two").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val health = if (metricSpec.withHealth) {
            Some(outlier < inlier)
          } else None
          val labels = Map(
            "modelVersionId" -> metricSpec.modelVersionId.toString,
            "trace" -> Traces.single(payload)
          )
          val metrics = Seq(
            Metric("gan_outlier", outlier, labels, health),
            Metric("gan_inlier", inlier, labels, health)
          )
          saveTo ! MetricWriter.ProcessedMetric(metrics)
        case Left(exc) => context.log.error(exc, s"Error while requesting GAN (${metricSpec.config.applicationName} -> ${metricSpec.config.applicationSignature}) prediction for modelVersion ${metricSpec.modelVersionId}")
      }
      this
  }
}
