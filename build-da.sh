#!/bin/bash

VERSION="1.0.0"
MODULE_NAME="da"
REGISTRY_URL="registry.seculayer.com:31500"

# docker build
DOCKER_BUILDKIT=1 docker build -t $REGISTRY_URL/ape/automl-$MODULE_NAME:$VERSION .
docker push $REGISTRY_URL/ape/automl-$MODULE_NAME:$VERSION
