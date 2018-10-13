#!/usr/bin/env groovy

import groovy.json.JsonOutput
//import junitxml

def slackNotificationChannel = "tmf_jenkins"

def notifySlacks(text, channel, attachments) {
    def slackURL = 'https://hooks.slack.com/services/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'
    def jenkinsIcon = 'https://wiki.jenkins-ci.org/download/attachments/2916393/logo.png'

    def payload = JsonOutput.toJson([text: text,
                                     channel: channel,
                                     username: "Jenkins",
                                     icon_url: jenkinsIcon,
                                     attachments: attachments
    ])

    sh "curl -X POST --data-urlencode \'payload=${payload}\' ${slackURL}"
}



def list = [
//            "extracts",
//            "library",
//            "recoding",
              "cph_fusion",
//            "tdf-final-feeds",
//            "tdf-final-validation",
//            "tdf-internet-digital-fusion",
//            "tdf-mdl-interface",
//            "tdf-post-fusion-processing",
//            "tdf-preinternet-home-fusion",
//            "tdf-preinternet-mobile-fusion",
//            "tdf-preinternet-work-fusion",
//            "tdf-pretablet-fusion",
//            "tdf-zerotv-fusion",
            ]

pipeline {
    agent {
        label 'master'
   }
    parameters {
        booleanParam(defaultValue: true, description: 'Is Artifactory upload required?', name: 'artifactory_upload')
        booleanParam(defaultValue: false, description: 'Is S3 upload required?', name: 's3_deploy')
    }
    // environment {
    //     JAVA_HOME='/mv_data/apps/jdk1.8'
    // }
    stages {
        
        stage("Initial Setup") {
            steps{
                script {
                for (item in list) {
                    sh "cd ${item} && ls -l"
                    echo "Starting with Build"
                }}
                 
                // notifySlack("", slackNotificationChannel, [
                //         [
                //                 "title": "${env.JOB_NAME}, build #${env.BUILD_NUMBER}",
                //                 "title_link": "${env.BUILD_URL}",
                //                 "color": "#FFFF00",
                //                 "text": "Build Started!",
                //                 "fields": [
                //                         [
                //                                 "title": "Branch",
                //                                 "value": "${env.GIT_BRANCH}",
                //                                 "short": true
                //                         ]
                //                 ]
                //         ]
                // ])
            }
        }
       
        stage('Unit test') {
            steps {
                script {
                for (item in list) {
                    sh "cd ${item}"
                    sh "cd ${item} && make test"
                    junit "${item}/src/test/*.xml"
                    echo "unit test is complete"
                }}

           }
   }
    stage('Build step') {
            steps {
                script {
                for (item in list) {
                    sh "cd ${item} && make clean"
                    sh "cd ${item} && python setup.py sdist bdist_wheel"
                    echo "build completed"
                }}
            }
    }
//        stage('Integration test') {
//            steps {
//                echo "Integration test is yet to implemented"
//            }
//}
stage('Upload to Artifactory') {
    when {
        expression {
            params.artifactory_upload == true
        }
    }
    steps {
        script {
        for (item in list) {
         //    sh "cd ${item} && . ./deploy.properties && curl -X PUT -T dist/tdf_cph_fusion-2.7.post27-py2-none-any.whl \${ARTIFACTORY_URL}\tdf_cph_fusion-2.7.post27-py2-none-any.whl"
            echo "Upload is completed"
        }}
    }
}
stage('Deploy to S3') {
   when {
       expression {
           params.s3_deploy == true
       }
   }
   steps {
       script {
       for (item in list) {
           sh "cd ${item} && . ./deploy.properties && cd dist && aws s3 sync . \${S3_PATH} "
           echo "S3 deploy is complete"
        }}
   }
}
}




post {

    success {
        echo "S3 deploy is complete"
        // notifySlack("", slackNotificationChannel, [
        //         [
        //                 "title": "${env.JOB_NAME}, build #${env.BUILD_NUMBER}",
        //                 "title_link": "${env.BUILD_URL}",
        //                 "color": "#00FF00",
        //                 "text": "Build Success!",
        //                 "fields": [
        //                         [
        //                                 "title": "Branch",
        //                                 "value": "${env.GIT_BRANCH}",
        //                                 "short": true
        //                         ]
        //                 ]
        //         ]
        // ])

    }
    failure {
        echo "S3 deploy is complete"
        // notifySlack("", slackNotificationChannel, [
        //         [
        //                 "title": "${env.JOB_NAME}, build #${env.BUILD_NUMBER}",
        //                 "title_link": "${env.BUILD_URL}",
        //                 "color": "#FF0000",
        //                 "text": "Build Failure!",
        //                 "fields": [
        //                         [
        //                                 "title": "Branch",
        //                                 "value": "${env.GIT_BRANCH}",
        //                                 "short": true
        //                         ]
        //                 ]
        //         ]
        // ])

    }
    unstable {
        echo "S3 deploy is complete"
        // notifySlack("", slackNotificationChannel, [
        //         [
        //                 "title": "${env.JOB_NAME}, build #${env.BUILD_NUMBER}",
        //                 "title_link": "${env.BUILD_URL}",
        //                 "color": "#FF0000",
        //                 "text": "Build Unstable!",
        //                 "fields": [
        //                         [
        //                                 "title": "Branch",
        //                                 "value": "${env.GIT_BRANCH}",
        //                                 "short": true
        //                         ]
        //                 ]
        //         ]
        // ])

    }

}
}
