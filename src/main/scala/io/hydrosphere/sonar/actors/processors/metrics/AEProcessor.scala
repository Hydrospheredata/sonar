package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.serving.tensorflow.TensorShape
import io.hydrosphere.serving.tensorflow.tensor.DoubleTensor
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms.{AEMetricSpec, Metric}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

class AEProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: AEMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {

  import Processor._

  override def onMessage(msg: MetricMessage): Behavior[MetricMessage] = msg match {
    case MetricRequest(payload, saveTo) =>
      context.log.debug(s"Computing AE: $payload")
      predictionService.callApplication(metricSpec.config.applicationName, metricSpec.config.applicationSignature,
        inputs = Map(
          "X" -> DoubleTensor(TensorShape.vector(-1), payload.getDoubleInput(metricSpec.config.input)).toProto
        )
      ).unsafeRunAsync {
        case Right(value) => 
          val reconstructed = value.outputs.get("reconstructed").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val health = if (metricSpec.withHealth) {
            Some(reconstructed <= metricSpec.config.threshold.getOrElse(Double.MaxValue))
          } else None
          val metric = Metric("autoencoder_reconstructed", reconstructed, Map("modelVersionId" -> metricSpec.modelVersionId.toString), health)
          saveTo ! MetricWriter.ProcessedMetric(Seq(metric))
        case Left(exc) => context.log.error(exc, s"Error while requesting AE (${metricSpec.config.applicationName} -> ${metricSpec.config.applicationSignature}) prediction for modelVersion ${metricSpec.modelVersionId}")
      }
      this
  }
}