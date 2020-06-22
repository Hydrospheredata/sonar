package io.hydrosphere.sonar.services

import java.net.URI
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import io.hydrosphere.serving.manager.grpc.entities.{Model, ModelVersion}
import io.hydrosphere.serving.monitoring.api.ExecutionInformation
import io.hydrosphere.sonar.terms.Check
import org.scalatest.FunSuite
import eu.timepit.refined._
import eu.timepit.refined.auto._
import io.hydrosphere.serving.monitoring.metadata.{ApplicationInfo, ExecutionMetadata}

class AlertManagerServiceTest extends FunSuite {

  // Local integration test

  ignore("Prometheus alert publishing") {
    val am: AlertService[IO] = new PrometheusAMService[IO](
      amUrl = "localhost:9093",
      baseUrl = "http://localhost:8080",
    )

    val modelId = 12
    val mvId = 33
    val modelName = "test-model"
    val mv = 1

    val execInfo = ExecutionInformation(
      metadata = ExecutionMetadata(
        modelVersionId = mvId,
        requestId = UUID.randomUUID().toString,
        modelName = modelName,
        modelVersion = mv,
        appInfo = ApplicationInfo(
          applicationId = 1,
          stageId = "stage-2"
        ).some
      ).some
    )
    val modelVersion = ModelVersion(
      id = mvId,
      version = mv,
      model = Model(modelId, modelName).some
    )
    val profileChecks = Map(
      "field_a" -> Seq(
        Check(true, "test a", 100, 50, None),
        Check(false, "test a", 50, 100, Some("spec-1"))
      ),
      "field_b" -> Seq(
        Check(true, "test b", 100, 50, Some("spec-1")),
        Check(false, "test b", 50, 100, None)
      )
    )
    val metricChecks = Map(
      "field_a" -> Check(false, "metric failed", 50, 100, None),
      "field_b" -> Check(true, "metric ok", 50, 100, None)
    )
    am.sendChecks(execInfo, modelVersion, profileChecks, metricChecks).unsafeRunSync()
  }
}