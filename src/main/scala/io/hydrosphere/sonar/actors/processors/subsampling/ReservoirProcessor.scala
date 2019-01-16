package io.hydrosphere.sonar.actors.processors.subsampling

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext}
import io.hydrosphere.serving.tensorflow.tensor.TensorProto
import io.hydrosphere.sonar.actors.Processor

import scala.util.Random

class ReservoirProcessor(context: ActorContext[Processor.SubsamplingMessage], size: Int) extends AbstractBehavior[Processor.SubsamplingMessage] {
  var reservoir: Array[Map[String, TensorProto]] = Array.ofDim[Map[String, TensorProto]](size)
  var count: Long = 0
  
  override def onMessage(msg: Processor.SubsamplingMessage): Behavior[Processor.SubsamplingMessage] = msg match {
    case Processor.SubsamplingRequest(payload) =>
      payload.request match {
        case Some(value) =>
          if (count >= size) {
            val p = size.toDouble / count.toDouble
            val r = Random.nextDouble()
            if (p < r) {
              val randomIndex = Random.nextInt(size)
              reservoir.update(randomIndex, value.inputs)
            }
          } else {
            reservoir.update(count.toInt, value.inputs)
          }
        case None => context.log.warning("Empty request")
      }       
      this
  }
}
