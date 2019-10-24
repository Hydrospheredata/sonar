package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.serving.tensorflow.TensorShape
import io.hydrosphere.serving.tensorflow.tensor.DoubleTensor
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms.{GANMetricSpec, Metric, MetricLabels}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

class GANProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: GANMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {
  override def onMessage(msg: Processor.MetricMessage): Behavior[Processor.MetricMessage] = msg match {
    case Processor.MetricRequest(payload, saveTo) =>
      context.log.debug(s"Computing GAN: $payload")
      predictionService.predict(metricSpec.id,
        inputs = Map(
          "client_profile" -> DoubleTensor(TensorShape.vector(112), payload.getDoubleInput(metricSpec.config.input)).toProto
        )
      ).unsafeRunAsync {
        case Right(value) =>
          val outlier = value.outputs.get("class_one").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val inlier = value.outputs.get("class_two").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val health = if (metricSpec.withHealth) {
            Some(outlier < inlier)
          } else None
          val labels = MetricLabels(
            modelVersionId = metricSpec.modelVersionId,
            metricSpecId = metricSpec.id,
            traces = Traces.single(payload),
            originTraces = OriginTraces.single(payload)
          )
          val metrics = Seq(
            Metric("gan_outlier", outlier, labels, health, payload.getTimestamp),
            Metric("gan_inlier", inlier, labels, health, payload.getTimestamp)
          )
          saveTo ! MetricWriter.ProcessedMetric(metrics)
        case Left(exc) => context.log.error(exc, s"Error while requesting GAN (${metricSpec.config.applicationName}) prediction for modelVersion ${metricSpec.modelVersionId}")
      }
      this
  }
}
