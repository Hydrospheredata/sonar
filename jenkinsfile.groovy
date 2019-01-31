def isRelease() {
  return env.IS_RELEASE_JOB == "true"
}

node("JenkinsOnDemand") {
    def organization = 'Provectus'
    def repository = 'sonar2'
    def accessTokenId = 'HydroRobot_AccessToken' 
    
    stage("Checkout") {
        def branches = (isRelease()) ? [[name: env.BRANCH_NAME]] : scm.branches
        checkout([
            $class: 'GitSCM',
            branches: branches,
            doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
            extensions: [[$class: 'CloneOption', noTags: false, shallow: false, depth: 0, reference: '']],
            userRemoteConfigs: scm.userRemoteConfigs,
       ])
   }

   stage('Build and test') {
      sh "sbt test"
   }

  
  if (isRelease()) {
    stage("Publish docker") {
      sh "sbt dockerBuildAndPush"
    } 
  }
}
