package io.hydrosphere.sonar.actors.processors.metrics

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import cats.data.NonEmptyList
import eu.timepit.refined.auto._
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.serving.tensorflow.api.predict.PredictRequest
import io.hydrosphere.sonar.actors.Processor
import io.hydrosphere.sonar.actors.writers.MetricWriter
import io.hydrosphere.sonar.terms.{KSMetricSpec, Metric}
import io.hydrosphere.sonar.utils.CollectionOps
import io.hydrosphere.sonar.utils.ExecutionInformationOps._
import io.hydrosphere.sonar.utils.math.{KolmogorovSmirnovTest, Statistics}

import scala.concurrent.duration.FiniteDuration

object KSProcessor {
  
  private case object TimerKey
  private case object Timeout extends Processor.MetricMessage
  
  private def filterRequest(request: Processor.MetricRequest, inputField: String): Processor.MetricRequest = {
    Processor.MetricRequest(
      payload = ExecutionInformation(
        request = for {
          r <- request.payload.request
          observingInput <- r.inputs.get(inputField)
        } yield PredictRequest(
          modelSpec = r.modelSpec,
          inputs = Map(inputField -> observingInput)
        ),
        metadata = request.payload.metadata,
        responseOrError = ExecutionInformation.ResponseOrError.Empty
      ),
      saveTo = request.saveTo
    )
  }
  
  private def computeMetrics(requests: Vector[Processor.MetricRequest], metricSpec: KSMetricSpec): Seq[Metric] = {
    val flatRequests = requests.map(r => r.payload.getDoubleInput(metricSpec.config.input))
    val ksFn = (sample: NonEmptyList[Double]) => KolmogorovSmirnovTest.test(sample, Statistics.generateDistribution(Statistics.Distribution.Normal, 100))
    val transposed = CollectionOps.safeTranspose(flatRequests)
    transposed.filter(_.nonEmpty).map(l => NonEmptyList(l.head, l.toList.tail)).map(ksFn).zipWithIndex.flatMap { case (ksResult, idx) =>
      
      val labels = Map(
        "modelVersionId" -> metricSpec.modelVersionId.toString,
        "columnIndex" -> idx.toString,
        "traces" -> Traces.many(requests.map(_.payload))
      )
      
      val health = if (metricSpec.withHealth) {
        Some(ksResult.value <= ksResult.rejectionLevel)
      } else None
      Seq(
        Metric("kolmogorovsmirnov", ksResult.value, labels, health),
        Metric("kolmogorovsmirnov_level", ksResult.rejectionLevel, labels, health)
      )
    }      
  }
  
  private def process(requests: Vector[Processor.MetricRequest], metricSpec: KSMetricSpec): Unit = {
    val metrics = computeMetrics(requests, metricSpec)
    val saveActors: Set[ActorRef[MetricWriter.Message]] = requests.map(_.saveTo).toSet
    saveActors.foreach(_ ! MetricWriter.ProcessedMetric(metrics))
  }
  
  
  def behavior(context: ActorContext[Processor.MetricMessage], metricSpec: KSMetricSpec, duration: FiniteDuration, maxSize: Int): Behavior[Processor.MetricMessage] = {
    Behaviors.withTimers { timers => 
      timers.startPeriodicTimer(TimerKey, Timeout, duration)
      active(Vector.empty[Processor.MetricRequest], timers, context, metricSpec, duration, maxSize)
    }
  }
  
  def active(buffer: Vector[Processor.MetricRequest], timers: TimerScheduler[Processor.MetricMessage], context: ActorContext[Processor.MetricMessage], metricSpec: KSMetricSpec, duration: FiniteDuration, maxSize: Int): Behavior[Processor.MetricMessage] = {
    Behaviors.receiveMessage {
      case m: Processor.MetricRequest => 
        val newBuffer = buffer :+ filterRequest(m, metricSpec.config.input)
        if (newBuffer.size == maxSize) {
          context.log.info(s"Processing KS for ${newBuffer.size} elements (max)")
          process(newBuffer, metricSpec)
          active(Vector.empty[Processor.MetricRequest], timers, context, metricSpec, duration, maxSize)
        } else {
          active(newBuffer, timers, context, metricSpec, duration, maxSize)
        }
      case Timeout =>
        context.log.info(s"Processing KS for ${buffer.size} elements (timeout)")
        process(buffer, metricSpec)
        active(Vector.empty[Processor.MetricRequest], timers, context, metricSpec, duration, maxSize)
    }
  }
}
