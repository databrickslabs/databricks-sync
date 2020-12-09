#!/bin/bash

alias dbsync='docker run --rm --name docker-terraformer -v "$PWD":/usr/src/databricks-terraformer -v ~/.databrickscfg:/root/.databrickscfg:ro -v ~/.ssh:/root/.ssh:ro -w /usr/src/databricks-terraformer databricks-terraformer'

dbsync export --profile azure_dr_source -c "$1" -l "$2"