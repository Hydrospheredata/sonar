package io.hydrosphere.sonar.config

case class Configuration(
                          mongo: MongoConfiguration,
                          grpc: GrpcConfiguration,
                          http: HttpConfiguration,
                          sidecar: SidecarConfiguration,
                          profile: ProfileConfiguration,
                          storage: StorageConfiguration,
                          alerting: Option[AlertingConfiguration]
)
