package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import cats.effect.IO
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.Processor.{MetricMessage, MetricRequest}
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms._
import io.hydrosphere.sonar.utils.ExecutionInformationOps._

class CustomModelProcessor(context: ActorContext[Processor.MetricMessage], metricSpec: CustomModelMetricSpec)(implicit predictionService: PredictionService[IO]) extends AbstractBehavior[Processor.MetricMessage] {

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
          predictionService.predict(metricSpec.config.servableName, request).unsafeRunAsync {
            case Right(response) =>
              val value = response.outputs.get("value").flatMap(_.doubleVal.headOption).getOrElse(0d)
              val health = if (metricSpec.withHealth) {
                metricSpec.config.thresholdCmpOperator match {
                  case Some(cmpOperator) => cmpOperator match {
                    case Eq => Some(value == metricSpec.config.threshold.getOrElse(Double.MaxValue))
                    case NotEq => Some(value != metricSpec.config.threshold.getOrElse(Double.MaxValue))
                    case Greater => Some(value > metricSpec.config.threshold.getOrElse(Double.MaxValue))
                    case Less => Some(value < metricSpec.config.threshold.getOrElse(Double.MaxValue))
                    case GreaterEq => Some(value >= metricSpec.config.threshold.getOrElse(Double.MaxValue))
                    case LessEq => Some(value <= metricSpec.config.threshold.getOrElse(Double.MaxValue))
                  }
                  case None => None
                }
              } else None
              val metric = Metric(
                "custom_model_value", value,
                MetricLabels(
                  modelVersionId = metricSpec.modelVersionId,
                  metricSpecId = metricSpec.id,
                  traces = Traces.single(payload),
                  originTraces = OriginTraces.single(payload)
                ),
                health,
                payload.getTimestamp)
              saveTo ! MetricWriter.ProcessedMetric(Seq(metric))
            case Left(exc) => context.log.error(exc, s"Error while requesting Custom Model prediction for modelVersion ${metricSpec.modelVersionId}")
          }
        case None => context.log.warning("Custom Model Metric Processor: executionInformation or response is empty")
      }
      this
  }
}
