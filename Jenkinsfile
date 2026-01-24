pipeline {
    agent any
    tools {
        jdk 'JDK 21'
    }
    stages {
        stage('Notify build in Slack') {
            steps {
                slackSend (color: '#D4DADF', message: "Started ${env.BUILD_URL}")
            }
        }
        stage('Build, test and package') {
            steps {
                sh './mvnw -B clean package'
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
                if (env.BRANCH_NAME == 'develop')
                    build job: '/NiFi.Dev.Builder', propagate: false, wait: false
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
