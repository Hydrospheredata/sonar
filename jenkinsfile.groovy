def repository = 'provectus/sonar2'

def buildAndPublishReleaseFunction = {
    //Buid app
    def curVersion = getVersion()
    sh "sbt -DappVersion=${curVersion} compile docker"
}

pipelineCommon(
        repository,
        false, //needSonarQualityGate,
        ["docker.hydrosphere.io/sonar"],
        {},
        buildAndPublishReleaseFunction,
        buildAndPublishReleaseFunction,
        buildAndPublishReleaseFunction,
        null,
        "hydro_private_docker_registry",
        "docker.hydrosphere.io",
        {},
        commitToCD("sonar")
)