package io.hydrosphere.sonar.services

import cats.effect.IO
import io.hydrosphere.serving.manager.grpc.entities.{Model, ModelVersion}
import org.scalatest.FunSuite

class AlertManagerServiceTest extends FunSuite {

  // Local integration test

//  test("Prometheus alert publishing") {
//    val modelDataService = new ModelDataService[IO] {
//      override def getModelVersion(modelVersionId: Long): IO[ModelVersion] = IO(ModelVersion(
//        id = 1,
//        version = 2,
//        model = Some(Model(id = 1, name = "bad-model"))
//      ))
//    }
//    val am = AlertManagerService.prometheus[IO](
//      amUrl = "localhost:9093",
//      baseUrl = "http://test-cluster:8080",
//      modelDataService = modelDataService
//    )
//    val metrics = List(
//      Metric("KS-test", 1, MetricLabels(1, "ks-metricspec-id", Nil, Nil), Some(true)),
//      Metric("KS-test", 2, MetricLabels(1, "ks-metricspec-id", Nil, Nil)),
//      Metric("KS-test", 3333, MetricLabels(1, "ks-metricspec-id", Nil, Nil), Some(false)),
//      Metric("Other-test", 4444, MetricLabels(1, "other-metricspec-id", Nil, Nil), Some(false)),
//      Metric("KS-test", 5, MetricLabels(1, "ks-metricspec-id", Nil, Nil), Some(false)),
//    )
//    am.sendMetric(metrics).unsafeRunSync()
//  }
}
