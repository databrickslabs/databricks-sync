## Jenkins Deployment

This guide gives indications on how to operate DB sync using Jenkins. The repo contains example of Jenkinsile and other necessary components.


### Configure Jenkins Agent
Install : 
* Docker
* Jenkins’ Docker plugins
* Databricks CLI

### Also :
* Set up ssh for git
* Make sure jenkins user has access to all needed commands
 
### Define environment variables
Define environment variables to be used in pipelines stage : 
* `DB_SYNC_REPO`: url to Git repository holding dbsync framework
* `DB_SYNC_VERSION`: stable version of the DR Framework
* `GIT_REPO`: url to Git repository holding custom DR configuration files and scripts
* `MASTER_REVISION` : stable version of customer DR framework
* `AZURE_SOURCE_WORKSPACE`: Source Workspace url
* `AZURE_TARGET_WORKSPACE` : Target Workspace url
* `LOCAL_GIT_REPO` : repository of Jenkins Job Name

```Groovy
  def DB_SYNC_REPO                   = "https://github.com/databrickslabs/databricks-sync"
  def DB_SYNC_VERSION                = "<dbsync-stable-release>"
  def GIT_REPO                       = "https://github.com/amineds/databricks-dr-demo"
  def MASTER_REVISION                = "MASTER_REVISION"
  def ARTIFACT_DIR                   = ""
  def AZURE_SOURCE_WORKSPACE         = "https://adb-455883741320485.5.azuredatabricks.net"
  def AZURE_TARGET_WORKSPACE         = "https://adb-7231012948833884.4.azuredatabricks.net"
  def LOCAL_GIT_REPO                 = "/var/lib/jenkins/workspace/${env.JOB_NAME}"
```

Note that sensitive data, like Databricks tokens, should be stored as credentials.

### Set up the pipeline
* Set up Databricks CLI for Source Workspace
* Set up Databricks CLI for Sink Workspace
* Build DB Sync Docker image

```Groovy
    stage('Setup') {
        withCredentials([
            string(credentialsId: DBSOURCETOKEN, variable: 'SOURCETOKEN'),
            string(credentialsId: DBSINKTOKEN, variable: 'SINKTOKEN')
        ]) {
            sh """#!/bin/bash
                # Configure Databricks CLI for source workspace
                echo "${DBSOURCEURL}
                $SOURCETOKEN" | databricks configure --token

                # Configure Databricks CLI for sink workspace
                echo "${DBSINKURL}
                $SINKTOKEN" | databricks configure --token

                # Set up the docker image
                git clone --branch ${DBSYNCRELEASE} ${DBSYNCFRAMEWORK}
                cd databricks-terraformer && docker build -t databricks-terraformer:latest .
            """
        }
    }
```

### Get the latest changes
Get last or specific version of organisation custom scripts and configs

```bash
    stage('Checkout') { 
        git branch: DBRELEASE, credentialsId: GITHUBCREDID, url: DRREPO
    }
```

### Export
Get last last changes from source Databricks workspace. 

Objects to be export and selection rules are compiled in a specific YAML file. Artifacts are generated in the jenkins job repository.

```yaml
#commit_msg_template:
    name: test
    objects:
      notebook:
        notebook_path: "/Shared/"
    #    patterns:
    #      - "*"
    ##    custom_map_vars:
    ##      path: "/Users/%{DATA:variable}/%{GREEDYDATA}"
    
      cluster_policy:
        patterns:
          - "*"
    ##TODO this map generated an error : Error: TypeError: 'list' object is not a mapping
    ##    custom_map_vars:
    ##        - "dat"
    #  driver_node_type_id: "%{GREEDYDATA:variable}"
    
      ##    custom_dynamic_vars:
      ##      - "format"
      dbfs_file:
        dbfs_path: "dbfs:/tests/"
    
      instance_pool:
        patterns:
          - "*"
      #    custom_map_vars:
      #      node_type_id: "%{GREEDYDATA:variable}"
      #  instance_profile:
      #    patterns:
      #      - "*"
      secret:
        patterns:
          - "*"
      identity:
        patterns:
          - "*"
      cluster:
        patterns:
          - "*"
      job:
        patterns:
          - "*"
```

```bash
    stage('Export') {
        sh "${LOCAL_GIT_REPO}/scripts/export.sh ${LOCAL_GIT_REPO}/scripts/migrate.yaml ${LOCAL_GIT_REPO}"
    }
```

### Import
Update sink Databricks workspace.

Objects are created / updated in the target repository based on artifacts generated in the export phase.

```bash
    stage('Import') {
        sh "${LOCAL_GIT_REPO}/scripts/import.sh ${LOCAL_GIT_REPO}" 
    }
```


```