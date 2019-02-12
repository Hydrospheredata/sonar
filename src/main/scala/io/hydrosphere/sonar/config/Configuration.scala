package io.hydrosphere.sonar.config

case class Configuration(
  db: DBConfiguration,
  mongo: MongoConfiguration,
  grpc: GrpcConfiguration,
  http: HttpConfiguration,
  sidecar: SidecarConfiguration,
  influx: InfluxConfiguration,
  subsampling: SubsamplingConfiguration,
  profile: ProfileConfiguration
)
