pipeline {
    agent any
    tools {
        jdk 'JDK 21'
    }
    stages {
        stage('Pre Build') {
            steps {
                slackSend (color: '#D4DADF', message: "Started ${env.BUILD_URL}")
            }
        }
        stage('Build and test') {
            steps {
                sh './mvnw -B package --file pom.xml'
            }
        }
    }
    post {
        always {
            junit 'nifi-ngsild-postgresql-processors/target/surefire-reports/*.xml'
            archiveArtifacts artifacts: 'nifi-ngsild-postgresql-nar/target/nifi-ngsild-postgresql-nar-*.nar', onlyIfSuccessful: true
        }
        success {
            script {
                if (env.BRANCH_NAME == 'main')
                    build job: 'NiFi.Prod.Builder'
                else if (env.BRANCH_NAME == 'develop')
                    build job: 'NiFi.Dev.Builder'
            }
            slackSend (color: '#36b37e', message: "Success: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
        unstable {
            slackSend (color: '#ff7f00', message: "Unstable: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
        failure {
            slackSend (color: '#FF0000', message: "Fail: ${env.BUILD_URL} after ${currentBuild.durationString.replace(' and counting', '')}")
        }
    }
}
