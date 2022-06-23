#!/bin/bash

set -e

TAG=pyo3-test
REPONAME=joshuarobinson

docker build -t $TAG --file Dockerfile .

# Tag and push to a public docker repository.
docker tag $TAG $REPONAME/$TAG
#docker push $REPONAME/$TAG
