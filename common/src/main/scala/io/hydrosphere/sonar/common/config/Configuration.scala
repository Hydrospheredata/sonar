package io.hydrosphere.sonar.common.config

import cats.effect.Sync
import cats.implicits._
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException
import eu.timepit.refined.pureconfig._
import pureconfig.generic.auto._


case class Configuration(
                          mongo: MongoConfiguration,
                          grpc: GrpcConfiguration,
                          http: HttpConfiguration,
                          sidecar: SidecarConfiguration,
                          profile: ProfileConfiguration,
                          storage: StorageConfiguration,
                          alerting: Option[AlertingConfiguration]
)

object Configuration {
  def load[F[_]: Sync]: F[Configuration] = Sync[F].fromEither(ConfigSource.default.load[Configuration].leftMap(ConfigReaderException(_))) 
}