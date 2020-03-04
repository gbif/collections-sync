pipeline {
  agent any
  tools {
    maven 'Maven3.2'
    jdk 'JDK8'
  }
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
  }
  parameters {
    booleanParam(name: 'RELEASE',
            defaultValue: false,
            description: 'Do a Maven release of the project.')
    booleanParam(name: 'SYNC',
            defaultValue: false,
            description: 'Run a sync')
    choice(name: 'ENV', choices: ['dev', 'uat', 'prod'], description: 'Choose environment')
  }
  stages {
    stage('Build') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.SYNC } };
        }
      }
      steps {
        configFileProvider(
                [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                        variable: 'MAVEN_SETTINGS_XML')]) {
          sh 'mvn clean package dependency:analyze -U'
        }
      }
    }
    stage('SonarQube analysis') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.SYNC } };
        }
      }
      steps {
        withSonarQubeEnv('GBIF Sonarqube') {
          sh 'mvn sonar:sonar'
        }
      }
    }
    stage('Snapshot to nexus') {
      when {
        allOf {
          not { expression { params.RELEASE } };
          not { expression { params.SYNC } };
          branch 'master';
        }
      }
      steps {
        configFileProvider(
                [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                        variable: 'MAVEN_SETTINGS_XML')]) {
          sh 'mvn -s $MAVEN_SETTINGS_XML deploy'
        }
      }
    }
    stage('Release version to nexus') {
      when { expression { params.RELEASE } }
      steps {
        configFileProvider(
                [configFile(fileId: 'org.jenkinsci.plugins.configfiles.maven.GlobalMavenSettingsConfig1387378707709',
                        variable: 'MAVEN_SETTINGS_XML')]) {
          git 'https://github.com/gbif/vocabulary.git'
          sh 'mvn -s $MAVEN_SETTINGS_XML -B release:prepare release:perform'
        }
      }
    }
    stage('Sync') {
      when { expression { params.SYNC } }
      steps {
        sshagent(['85f1747d-ea03-49ca-9e5d-aa9b7bc01c5f']) {
          sh """
           rm -rf *
           git clone -b master git@github.com:gbif/gbif-configuration.git
           curl "https://repository.gbif.org/service/rest/v1/search/assets/download?repository=gbif&group=org.gbif\
                &name=collections-sync&sort=version&direction=desc&prerelease=false&maven.classifier&maven.extension=jar" \
                -L -o collections-sync.jar
           java -jar collections-sync.jar -c gbif-configuration/collections-sync/${params.ENV.toLowerCase()}/config.yaml
          """
          archiveArtifacts artifacts: '/**/ih_sync_result_*', fingerprint: true, allowEmptyArchive: true
        }
      }
    }
  }

}