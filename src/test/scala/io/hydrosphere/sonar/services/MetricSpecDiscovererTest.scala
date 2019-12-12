package io.hydrosphere.sonar.services

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import com.google.protobuf.empty.Empty
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.discovery.serving.ServingDiscoveryGrpc.ServingDiscovery
import io.hydrosphere.serving.discovery.serving.{ApplicationDiscoveryEvent, MetricSpecDiscoveryEvent, ServableDiscoveryEvent}
import io.hydrosphere.serving.manager.grpc.entities.{CustomModelMetric, MetricSpec, Servable, ThresholdConfig}
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer
import io.hydrosphere.sonar.actors.MetricSpecDiscoverer.{GetAll, GetAllResponse}
import org.scalatest.FunSpec

import scala.concurrent.duration._

class MetricSpecDiscovererTest extends FunSpec {
  describe("MetricSpec actor") {
    val spec = MetricSpec(
      id = "test",
      customModelConfig = Some(CustomModelMetric(
        monitorModelId = 2,
        threshold = Some(ThresholdConfig(12, ThresholdConfig.CmpOp.EQ)),
        servable = Some(Servable())
      )))

    val stub = new ServingDiscovery {
      override def watchApplications(responseObserver: StreamObserver[ApplicationDiscoveryEvent]): StreamObserver[Empty] = ???

      override def watchServables(responseObserver: StreamObserver[ServableDiscoveryEvent]): StreamObserver[Empty] = ???

      override def watchMetricSpec(responseObserver: StreamObserver[MetricSpecDiscoveryEvent]): StreamObserver[Empty] = {
        responseObserver.onNext(MetricSpecDiscoveryEvent(added=List(spec)))
        new StreamObserver[Empty] {
          override def onNext(value: Empty): Unit = ()

          override def onError(t: Throwable): Unit = ()

          override def onCompleted(): Unit = ()
        }
      }
    }

    it("should connect in disconnected state") {
      val testKit = ActorTestKit()
      val actor = testKit.spawn(MetricSpecDiscoverer(5.seconds, stub))
      val probe = testKit.createTestProbe[GetAllResponse]()
      actor ! GetAll(probe.ref)
      val emptyRes = probe.receiveOne()
      assert(emptyRes.specs.isEmpty)
      actor ! MetricSpecDiscoverer.DiscoveredSpec(MetricSpecDiscoveryEvent(added = List(spec)))
//      Thread.sleep(5000)
      val probe2 = testKit.createTestProbe[GetAllResponse]()

      actor ! GetAll(probe2.ref)
      val discovered = probe2.receiveOne(10.seconds)
      assert(discovered.specs.head.id == "test")

      testKit.shutdownTestKit()
    }
  }
}
