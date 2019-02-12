package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.serving.tensorflow.TensorShape
import io.hydrosphere.serving.tensorflow.tensor.DoubleTensor
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms.{Metric, RFMetricSpec}
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

class RFProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: RFMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {
  import Processor._

  override def onMessage(msg: MetricMessage): Behavior[MetricMessage] = msg match {
    case MetricRequest(payload, saveTo) =>
      context.log.debug(s"Computing RF: $payload")
      predictionService.callApplication(metricSpec.config.applicationName, metricSpec.config.applicationSignature,
        inputs = Map(
          "features" -> DoubleTensor(TensorShape.vector(-1), payload.getDoubleInput(metricSpec.config.input)).toProto
        )
      ).unsafeRunAsync {
        case Right(value) =>
          val score = value.outputs.get("score").flatMap(_.doubleVal.headOption).getOrElse(0d)
          val health = if (metricSpec.withHealth) {
            Some(score <= metricSpec.config.threshold.getOrElse(Double.MaxValue))
          } else None
          
          val labels = Map(
            "modelVersionId" -> metricSpec.modelVersionId.toString,
            "trace" -> Traces.single(payload)
          )
          val metric = Metric("randomforest", score, labels, health)
          saveTo ! MetricWriter.ProcessedMetric(Seq(metric))
        case Left(exc) => context.log.error(exc, s"Error while requesting RF prediction (${metricSpec.config.applicationName} -> ${metricSpec.config.applicationSignature}) for modelVersion ${metricSpec.modelVersionId}")
      }
      this
  }
}
