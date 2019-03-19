package io.hydrosphere.sonar.utils

import akka.actor.typed.scaladsl.Behaviors
import cats.effect.IO
import io.hydrosphere.sonar.actors.processors.metrics._
import io.hydrosphere.sonar.services.PredictionService
import io.hydrosphere.sonar.terms._

import scala.concurrent.duration._

object ProcessableMetricSpecs {
  implicit def processableKS: Processable[KSMetricSpec] = (t: KSMetricSpec) => Behaviors.setup(context => {
    KSProcessor.behavior(context, t, 1 minute, 10)
  })
  
  implicit def processableRF(implicit predictionService: PredictionService[IO]): Processable[RFMetricSpec] = (t: RFMetricSpec) => Behaviors.setup(context => {
    new RFProcessor(context, t)
  })
  
  implicit def processableAE(implicit predictionService: PredictionService[IO]): Processable[AEMetricSpec] = (t: AEMetricSpec) => Behaviors.setup(context => {
    new AEProcessor(context, t)
  })

  implicit def processableImageAE(implicit predictionService: PredictionService[IO]): Processable[ImageAEMetricSpec] = (t: ImageAEMetricSpec) => Behaviors.setup(context => {
    new ImageAEProcessor(context, t)
  })

  implicit def processableGAN(implicit predictionService: PredictionService[IO]): Processable[GANMetricSpec] = (t: GANMetricSpec) => Behaviors.setup(context => {
    new GANProcessor(context, t)
  })
  
  implicit def processableLatency: Processable[LatencyMetricSpec] = (t: LatencyMetricSpec) => Behaviors.setup(context => {
    LatencyProcessor.behavior(context, t, FiniteDuration(t.config.interval, "seconds"))
  })
  
  implicit def processableCounter: Processable[CounterMetricSpec] = (t: CounterMetricSpec) => Behaviors.setup(context => {
    CounterProcessor.behavior(context, t, FiniteDuration(t.config.interval, "seconds"))
  })
  
  implicit def processableErrorRate: Processable[ErrorRateMetricSpec] = (t: ErrorRateMetricSpec) => Behaviors.setup(context => {
    ErrorRateProcessor.behavior(context, t, FiniteDuration(t.config.interval, "seconds"))
  })
}
